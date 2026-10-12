<?php

declare(strict_types=1);

/**
 * RunRemoting SOCKS 代理用例的子进程：在一个进程里同时扮演
 *   1) 目标 echo 服务（连接后原样回显字节，父进程用 RocketMQ 帧做请求/响应断言）
 *   2) 假 SOCKS 代理（按 $argv[1] 的模式做最小握手 + CONNECT，成功后把两条流对接）
 *
 * 启动后先向 stdout 打一行 ``READY <proxyPort> <targetPort>`` 再进入服务循环；
 * 父进程读这行拿端口。父进程关闭客户端连接 → 隧道结束 → 本进程自然退出；
 * 测试失败路径由父进程 proc_terminate 兜底。
 *
 * 模式：
 *   socks5_ok            SOCKS5 无认证
 *   socks5_auth          SOCKS5 用户名/密码子协商（期望 user/pass 来自 $argv[2]/[3]）
 *   socks5_reject_auth   子协商返回失败状态（0x01）
 *   socks5_reject_connect 方法协商通过、CONNECT 回 REP=0x01（general failure）
 *   socks4_ok            SOCKS4（无 userid 校验）
 *   socks4_reject        CONNECT 回 CD=91
 */

$mode = $argv[1] ?? '';
$user = $argv[2] ?? '';
$pass = $argv[3] ?? '';

// ---- 两个监听先就位，端口报给父进程 ----
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

// ---- 阻塞读 n 字节（本地环回、父进程就在隔壁，无需超时兜底） ----
function sockReadExact($conn, int $need): string
{
    $buf = '';
    while (strlen($buf) < $need) {
        $chunk = fread($conn, $need - strlen($buf));
        if ($chunk === false || $chunk === '') {
            if (feof($conn)) {
                exit(3); // 客户端提前断开
            }
            continue;
        }
        $buf .= $chunk;
    }
    return $buf;
}

/**
 * 隧道数据面：pConn(客户端↔代理) ↔ tConn(代理拨向目标的连接) ↔ srv(目标 echo 服务侧)。
 * 三条流一起 select：pConn → tConn，tConn → pConn，srv 回显到 srv。
 * 注意非阻塞流 fread 返回 ''/false 只是暂时无数据，以 feof 定夺真 EOF（Windows 同款怪癖）。
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

// ---- 接一个代理连接并按模式握手 ----
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
        // 1) 方法协商
        $head = unpack('Cver/Cn', sockReadExact($pConn, 2));
        if ($head['ver'] !== 5) {
            exit(5);
        }
        sockReadExact($pConn, $head['n']);
        if ($mode === 'socks5_ok' || $mode === 'socks5_reject_connect') {
            // reject_connect 也走无认证 greeting：卡点在 CONNECT 的 REP，不在方法协商
            fwrite($pConn, "\x05\x00");
        } else {
            // 其余三个模式都要求走用户名/密码子协商
            fwrite($pConn, "\x05\x02");
            $aVer = unpack('Cver/Culen', sockReadExact($pConn, 2));
            $aUser = sockReadExact($pConn, $aVer['ulen']);
            $aPassLen = unpack('Cplen', sockReadExact($pConn, 1));
            $aPass = sockReadExact($pConn, $aPassLen['plen']);
            if ($mode === 'socks5_reject_auth' || $aUser !== $user || $aPass !== $pass) {
                fwrite($pConn, "\x01\x01");
                exit(0); // 认证失败，父进程应在客户端侧看到握手异常
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
        // 3) 连目标，回成功，开始对拷
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
        // userid 读到 0x00 结尾（本端口测试不带 userid）
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
