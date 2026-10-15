<?php

declare(strict_types=1);

/**
 * Child process for the RunRemoting SOCKS proxy cases: a single process playing two roles,
 *   1) the target echo service (echoes bytes verbatim after connect; the parent asserts
 *      requests/responses with RocketMQ frames)
 *   2) a fake SOCKS proxy (minimal handshake + CONNECT per the $argv[1] mode; on success it
 *      splices the two streams together)
 *
 * On startup it first prints one line ``READY <proxyPort> <targetPort>`` to stdout, then enters
 * the service loop; the parent reads that line to get the ports. When the parent closes the
 * client connection -> the tunnel ends -> this process exits naturally; failure paths are backed
 * up by the parent's proc_terminate.
 *
 * Modes:
 *   socks5_ok            SOCKS5 without authentication
 *   socks5_auth          SOCKS5 username/password sub-negotiation (expects user/pass from $argv[2]/[3])
 *   socks5_reject_auth   the sub-negotiation returns a failure status (0x01)
 *   socks5_reject_connect method negotiation passes, CONNECT replies REP=0x01 (general failure)
 *   socks4_ok            SOCKS4 (no userid validation)
 *   socks4_reject        CONNECT replies CD=91
 */

$mode = $argv[1] ?? '';
$user = $argv[2] ?? '';
$pass = $argv[3] ?? '';

// ---- Both listeners in place first, then report the ports to the parent ----
$target = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if ($target === false) {
    fwrite(STDERR, "target listen failed: $errstr\n");
    exit(2);
}
$proxy = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
if ($proxy === false) {
    fwrite(STDERR, "proxy listen failed: $errstr\n");
    exit(2);
}
$tName = (string)stream_socket_get_name($target, false);
$pName = (string)stream_socket_get_name($proxy, false);
fwrite(STDOUT, sprintf("READY %d %d\n", (int)substr($pName, strrpos($pName, ':') + 1), (int)substr($tName, strrpos($tName, ':') + 1)));
fflush(STDOUT);

// ---- Blocking read of n bytes (local loopback, the parent is right next door; no timeout backstop needed) ----
function sockReadExact($conn, int $need): string
{
    $buf = '';
    while (strlen($buf) < $need) {
        $chunk = fread($conn, $need - strlen($buf));
        if ($chunk === false || $chunk === '') {
            if (feof($conn)) {
                exit(3); // client disconnected early
            }
            continue;
        }
        $buf .= $chunk;
    }
    return $buf;
}

/**
 * Tunnel data plane: pConn (client<->proxy) <-> tConn (proxy dials the target) <-> srv (the
 * target echo service side). All three streams are selected together: pConn -> tConn,
 * tConn -> pConn, srv echoes to srv.
 * Note that on non-blocking streams fread returning ''/false only means "no data for now"; feof
 * decides a real EOF (the same quirk as on Windows).
 */
function tunnelEcho($pConn, $tConn, $targetServer): void
{
    $srv = @stream_socket_accept($targetServer, 10);
    if ($srv === false) {
        fwrite(STDERR, "target accept failed\n");
        return;
    }
    foreach ([$pConn, $tConn, $srv] as $s) {
        stream_set_blocking($s, false);
    }
    $streams = [$pConn, $tConn, $srv];
    while (true) {
        $r = $streams;
        $w = [];
        $e = [];
        $n = @stream_select($r, $w, $e, 10);
        if ($n === false || $n === 0) {
            continue;
        }
        foreach ($r as $src) {
            $data = fread($src, 65536);
            if ($data === false || $data === '') {
                if (feof($src)) {
                    return;
                }
                continue;
            }
            $dst = $src === $pConn ? $tConn : ($src === $tConn ? $pConn : $srv);
            fwrite($dst, $data);
        }
    }
}

// ---- Accept one proxy connection and shake hands per the mode ----
$pConn = @stream_socket_accept($proxy, 30);
if ($pConn === false) {
    fwrite(STDERR, "proxy accept timeout\n");
    exit(4);
}

$handshakeOk = false;
switch ($mode) {
    case 'socks5_ok':
    case 'socks5_auth':
    case 'socks5_reject_auth':
    case 'socks5_reject_connect':
        // 1) Method negotiation
        $head = unpack('Cver/Cn', sockReadExact($pConn, 2));
        if ($head['ver'] !== 5) {
            exit(5);
        }
        sockReadExact($pConn, $head['n']);
        if ($mode === 'socks5_ok' || $mode === 'socks5_reject_connect') {
            // reject_connect also uses the no-auth greeting: the sticking point is CONNECT's REP,
            // not the method negotiation
            fwrite($pConn, "\x05\x00");
        } else {
            // The other three modes all require the username/password sub-negotiation
            fwrite($pConn, "\x05\x02");
            $aVer = unpack('Cver/Culen', sockReadExact($pConn, 2));
            $aUser = sockReadExact($pConn, $aVer['ulen']);
            $aPassLen = unpack('Cplen', sockReadExact($pConn, 1));
            $aPass = sockReadExact($pConn, $aPassLen['plen']);
            if ($mode === 'socks5_reject_auth' || $aUser !== $user || $aPass !== $pass) {
                fwrite($pConn, "\x01\x01");
                exit(0); // auth failed; the parent should see a handshake exception on the client side
            }
            fwrite($pConn, "\x01\x00");
        }
        // 2) CONNECT
        $req = unpack('Cver/Ccmd/Crsv/Catyp', sockReadExact($pConn, 4));
        $addrLen = match ($req['atyp']) {
            0x01 => 4,
            0x03 => ord(sockReadExact($pConn, 1)),
            0x04 => 16,
            default => exit(6),
        };
        $dstAddr = sockReadExact($pConn, $addrLen);
        $dstPort = unpack('nport', sockReadExact($pConn, 2))['port'];
        if ($req['ver'] !== 5 || $req['cmd'] !== 1) {
            exit(7);
        }
        if ($mode === 'socks5_reject_connect') {
            fwrite($pConn, "\x05\x01\x00\x01" . str_repeat("\x00", 6));
            exit(0);
        }
        // 3) Connect to the target, reply success, start the byte-for-byte copy
        $tConn = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $dstPort), $errno, $errstr, 5);
        if ($tConn === false) {
            fwrite($pConn, "\x05\x05\x00\x01" . str_repeat("\x00", 6)); // connection refused
            exit(8);
        }
        fwrite($pConn, "\x05\x00\x00\x01" . str_repeat("\x00", 6));
        $handshakeOk = true;
        tunnelEcho($pConn, $tConn, $target);
        fclose($tConn);
        break;

    case 'socks4_ok':
    case 'socks4_reject':
        $req = unpack('Cver/Ccmd/nport/Nip', sockReadExact($pConn, 8));
        // userid reads until 0x00 (this port's tests carry no userid)
        while (true) {
            $c = sockReadExact($pConn, 1);
            if ($c === "\x00") {
                break;
            }
        }
        if ($req['ver'] !== 4 || $req['cmd'] !== 1) {
            exit(9);
        }
        if ($mode === 'socks4_reject') {
            fwrite($pConn, "\x00\x5b" . str_repeat("\x00", 6));
            exit(0);
        }
        $tConn = @stream_socket_client(sprintf('tcp://127.0.0.1:%d', $req['port']), $errno, $errstr, 5);
        if ($tConn === false) {
            fwrite($pConn, "\x00\x5b" . str_repeat("\x00", 6)); // 91 = failed
            exit(10);
        }
        fwrite($pConn, "\x00\x5a" . pack('n', $req['port']) . pack('N', ip2long('127.0.0.1')));
        $handshakeOk = true;
        tunnelEcho($pConn, $tConn, $target);
        fclose($tConn);
        break;

    default:
        fwrite(STDERR, "unknown mode: $mode\n");
        exit(1);
}

fclose($pConn);
exit($handshakeOk ? 0 : 11);
