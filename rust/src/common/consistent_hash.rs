//! ## 为什么整套照搬而不是"等价改写"

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{Error, Result};

pub trait HashFunction: Send + Sync {
    fn hash(&self, key: &str) -> i64;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Md5Hash;

impl HashFunction for Md5Hash {
    /// 只取摘要前 4 字节、大端拼接（见模块头第 1 点）。结果因此恒在 `0..=0xFFFFFFFF`。
    fn hash(&self, key: &str) -> i64 {
        let digest = md5(key.as_bytes());
        let mut value: i64 = 0;
        for byte in digest.iter().take(4) {
            value = (value << 8) | i64::from(*byte);
        }
        value
    }
}

pub trait Node: Send + Sync {
    fn get_key(&self) -> &str;
}

#[derive(Debug, Clone)]
pub struct ClientNode {
    client_id: String,
}

impl ClientNode {
    pub fn new(client_id: &str) -> ClientNode {
        ClientNode {
            client_id: client_id.to_string(),
        }
    }

    pub fn get_client_id(&self) -> &str {
        &self.client_id
    }
}

impl Node for ClientNode {
    fn get_key(&self) -> &str {
        &self.client_id
    }
}

#[derive(Clone)]
struct VirtualNode {
    physical: Arc<dyn Node>,
    replica_index: i32,
}

impl VirtualNode {
    fn key(&self) -> String {
        format!("{}-{}", self.physical.get_key(), self.replica_index)
    }

    fn is_virtual_node_of(&self, p_node: &dyn Node) -> bool {
        self.physical.get_key() == p_node.get_key()
    }
}

pub struct ConsistentHashRouter {
    ring: BTreeMap<i64, VirtualNode>,
    hash_function: Arc<dyn HashFunction>,
}

/// 手写而不是 `#[derive(Debug)]`：`Arc<dyn HashFunction>` 没有 `Debug`，派生会因它失败。
/// 环里存的是 hash → 虚拟节点，打印时只列 hash 与虚拟节点 key，不外泄业务节点对象。
impl std::fmt::Debug for ConsistentHashRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ring: Vec<(i64, String)> = self
            .ring
            .iter()
            .map(|(hash, v_node)| (*hash, v_node.key()))
            .collect();
        f.debug_struct("ConsistentHashRouter")
            .field("ring", &ring)
            .finish()
    }
}

impl ConsistentHashRouter {
    pub fn new(p_nodes: &[Arc<dyn Node>], v_node_count: i32) -> Result<ConsistentHashRouter> {
        Self::with_hash_function(p_nodes, v_node_count, Arc::new(Md5Hash))
    }

    pub fn with_hash_function(
        p_nodes: &[Arc<dyn Node>],
        v_node_count: i32,
        hash_function: Arc<dyn HashFunction>,
    ) -> Result<ConsistentHashRouter> {
        let mut router = ConsistentHashRouter {
            ring: BTreeMap::new(),
            hash_function,
        };
        for p_node in p_nodes {
            router.add_node(&**p_node, v_node_count)?;
        }
        Ok(router)
    }

    pub fn add_node(&mut self, p_node: &dyn Node, v_node_count: i32) -> Result<()> {
        if v_node_count < 0 {
            return Err(Error::client(format!(
                "illegal virtual node counts :{v_node_count}"
            )));
        }
        let existing_replicas = self.get_existing_replicas(p_node);
        for i in 0..v_node_count {
            let v_node = VirtualNode {
                physical: shared_node_of(p_node),
                replica_index: i + existing_replicas,
            };
            let hash = self.hash_function.hash(&v_node.key());
            self.ring.insert(hash, v_node);
        }
        Ok(())
    }

    pub fn remove_node(&mut self, p_node: &dyn Node) {
        let dead: Vec<i64> = self
            .ring
            .iter()
            .filter(|(_, v_node)| v_node.is_virtual_node_of(p_node))
            .map(|(hash, _)| *hash)
            .collect();
        for hash in dead {
            self.ring.remove(&hash);
        }
    }

    pub fn route_node(&self, object_key: &str) -> Option<Arc<dyn Node>> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = self.hash_function.hash(object_key);
        let (_, v_node) = match self.ring.range(hash..).next() {
            Some(entry) => entry,
            None => self.ring.iter().next()?,
        };
        Some(v_node.physical.clone())
    }

    pub fn get_existing_replicas(&self, p_node: &dyn Node) -> i32 {
        self.ring
            .values()
            .filter(|v_node| v_node.is_virtual_node_of(p_node))
            .count() as i32
    }
}

/// `add_node` 吃 `&dyn Node`（方便测试与内部复用），环里却要存 `Arc<dyn Node>`。
fn shared_node_of(p_node: &dyn Node) -> Arc<dyn Node> {
    Arc::new(ClientNode::new(p_node.get_key()))
}

// ------------------------------------------------------------------ MD5

/// MD5 的 64 轮位移量（RFC 1321 §3.4）。
const MD5_SHIFTS: [u32; 64] = [
    7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, //
    5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, //
    4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, //
    6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, //
];

/// `K[i] = floor(abs(sin(i + 1)) * 2^32)`（RFC 1321 §3.4 的表）。
const MD5_K: [u32; 64] = [
    0xd76a_a478, 0xe8c7_b756, 0x2420_70db, 0xc1bd_ceee, //
    0xf57c_0faf, 0x4787_c62a, 0xa830_4613, 0xfd46_9501, //
    0x6980_98d8, 0x8b44_f7af, 0xffff_5bb1, 0x895c_d7be, //
    0x6b90_1122, 0xfd98_7193, 0xa679_438e, 0x49b4_0821, //
    0xf61e_2562, 0xc040_b340, 0x265e_5a51, 0xe9b6_c7aa, //
    0xd62f_105d, 0x0244_1453, 0xd8a1_e681, 0xe7d3_fbc8, //
    0x21e1_cde6, 0xc337_07d6, 0xf4d5_0d87, 0x455a_14ed, //
    0xa9e3_e905, 0xfcef_a3f8, 0x676f_02d9, 0x8d2a_4c8a, //
    0xfffa_3942, 0x8771_f681, 0x6d9d_6122, 0xfde5_380c, //
    0xa4be_ea44, 0x4bde_cfa9, 0xf6bb_4b60, 0xbebf_bc70, //
    0x289b_7ec6, 0xeaa1_27fa, 0xd4ef_3085, 0x0488_1d05, //
    0xd9d4_d039, 0xe6db_99e5, 0x1fa2_7cf8, 0xc4ac_5665, //
    0xf429_2244, 0x432a_ff97, 0xab94_23a7, 0xfc93_a039, //
    0x655b_59c3, 0x8f0c_cc92, 0xffef_f47d, 0x8584_5dd1, //
    0x6fa8_7e4f, 0xfe2c_e6e0, 0xa301_4314, 0x4e08_11a1, //
    0xf753_7e82, 0xbd3a_f235, 0x2ad7_d2bb, 0xeb86_d391, //
];

fn md5(input: &[u8]) -> [u8; 16] {
    let mut state = [
        0x6745_2301u32,
        0xefcd_ab89,
        0x98ba_dcfe,
        0x1032_5476,
    ];

    // 填充：0x80 + 若干 0x00 把长度顶到 ≡56 (mod 64)，再挂 8 字节小端 bit 长度。
    let bit_len = (input.len() as u64).wrapping_mul(8);
    let mut padded = input.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0x00);
    }
    padded.extend_from_slice(&bit_len.to_le_bytes());

    for block in padded.chunks(64) {
        let mut words = [0u32; 16];
        for (i, word) in words.iter_mut().enumerate() {
            let at = i * 4;
            *word = u32::from_le_bytes([block[at], block[at + 1], block[at + 2], block[at + 3]]);
        }

        let [mut a, mut b, mut c, mut d] = state;
        for i in 0..64 {
            // 四轮的非线性函数与消息字下标（RFC 1321 §3.4）。
            let (f, g) = match i {
                0..=15 => ((b & c) | ((!b) & d), i),
                16..=31 => ((d & b) | ((!d) & c), (5 * i + 1) % 16),
                32..=47 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | (!d)), (7 * i) % 16),
            };
            let next = f
                .wrapping_add(a)
                .wrapping_add(MD5_K[i])
                .wrapping_add(words[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(next.rotate_left(MD5_SHIFTS[i]));
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
    }

    let mut out = [0u8; 16];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MD5 摘要的小写十六进制串（只做测试里的逐字节比对用，故不放进生产代码里）。
    fn md5_hex(input: &[u8]) -> String {
        md5(input).iter().map(|b| format!("{b:02x}")).collect()
    }

    /// RFC 1321 附录 A 的官方测试向量（另加一条长输入向量）。
    #[test]
    fn md5_matches_the_reference_vectors() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(md5_hex(b"a"), "0cc175b9c0f1b6a831c399e269772661");
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(md5_hex(b"message digest"), "f96b697d7cb7938d525a2f31aaf161d0");
        assert_eq!(
            md5_hex(b"abcdefghijklmnopqrstuvwxyz"),
            "c3fcd3d76192e4007dfb496cca67e13b"
        );
        assert_eq!(
            md5_hex(b"The quick brown fox jumps over the lazy dog"),
            "9e107d9d372bb6826bd81d3542a419d6"
        );
        // 跨块 + 正好落在填充边界的几种长度（55/56/57），参考值按 MD5 定义算出
        for (len, expected) in [
            (55usize, "04364420e25c512fd958a70738aa8f72"),
            (56, "668a72d5ba17f08e62dabcafad6db14b"),
            (57, "693037871c4a9d3d8685018905cb530a"),
            (64, "c1bb4f81d892b2d57947682aeb252456"),
            (65, "1bc932052302d074bdec39795fe00cf6"),
            (200, "30a83621ce5422fbdfdd539777458c78"),
        ] {
            assert_eq!(md5_hex(&vec![b'x'; len]), expected, "len={len}");
        }
    }

    #[test]
    fn ring_md5_hash_takes_only_the_first_four_bytes() {
        assert_eq!(Md5Hash.hash(""), 0xd41d_8cd9);
        assert_eq!(Md5Hash.hash("abc"), 0x9001_5098);
        assert_eq!(Md5Hash.hash("abc") >> 32, 0, "结果必须在 32 位内（只取 4 个字节）");
    }

    #[test]
    fn ring_routes_to_the_clockwise_neighbour_and_wraps_around() {
        let nodes: Vec<Arc<dyn Node>> =
            vec![Arc::new(ClientNode::new("n0")), Arc::new(ClientNode::new("n1"))];
        let router = ConsistentHashRouter::new(&nodes, 5).unwrap();
        // 同一个 key 永远落在同一个节点上（策略的稳定性前提）
        for key in ["MessageQueue [topic=t, brokerName=b, queueId=0]", "k", "另一个"] {
            let first = router.route_node(key).unwrap().get_key().to_string();
            for _ in 0..3 {
                assert_eq!(router.route_node(key).unwrap().get_key(), first, "{key}");
            }
        }
        // 回绕分支：环上一定取得到节点，且两个节点都被路由到（10 个虚拟节点足够密）
        let mut seen: Vec<String> = Vec::new();
        for i in 0..64 {
            let routed = router.route_node(&format!("queue-{i}")).unwrap().get_key().to_string();
            if !seen.contains(&routed) {
                seen.push(routed);
            }
        }
        assert_eq!(seen.len(), 2, "两个物理节点都得被路由到: {seen:?}");
    }

    #[test]
    fn empty_ring_routes_to_nothing() {
        let router = ConsistentHashRouter::new(&[], 3).unwrap();
        assert!(router.route_node("anything").is_none());
    }

    #[test]
    fn negative_virtual_node_count_is_rejected() {
        let nodes: Vec<Arc<dyn Node>> = vec![Arc::new(ClientNode::new("a"))];
        assert!(ConsistentHashRouter::new(&[], -1).is_ok(), "空节点集不触发检查");
        let err = ConsistentHashRouter::new(&nodes, -1).expect_err("必须拒绝");
        assert!(
            err.to_string().contains("illegal virtual node counts :-1"),
            "{err}"
        );
    }

    #[test]
    fn re_adding_a_node_keeps_virtual_nodes_distinct() {
        let node = ClientNode::new("dup");
        let mut router = ConsistentHashRouter::new(&[], 0).unwrap();
        router.add_node(&node, 3).unwrap();
        assert_eq!(router.get_existing_replicas(&node), 3);
        router.add_node(&node, 2).unwrap();
        assert_eq!(router.get_existing_replicas(&node), 5);
        assert_eq!(router.ring.len(), 5, "5 个虚拟节点必须占 5 个不同的 hash");
    }

    #[test]
    fn remove_node_drops_only_that_physical_node() {
        let a = ClientNode::new("a");
        let b = ClientNode::new("b");
        let nodes: Vec<Arc<dyn Node>> =
            vec![Arc::new(ClientNode::new("a")), Arc::new(ClientNode::new("b"))];
        let mut router = ConsistentHashRouter::new(&nodes, 4).unwrap();
        router.remove_node(&a);
        assert_eq!(router.get_existing_replicas(&a), 0);
        assert_eq!(router.get_existing_replicas(&b), 4);
        assert_eq!(router.route_node("whatever").unwrap().get_key(), "b");
    }

    #[test]
    fn custom_hash_function_is_used_for_every_lookup() {
        // 只用首字符的哈希：环上落点完全由 key 的第一个字节决定
        struct FirstByte;
        impl HashFunction for FirstByte {
            fn hash(&self, key: &str) -> i64 {
                key.bytes().next().unwrap_or(0) as i64
            }
        }
        let nodes: Vec<Arc<dyn Node>> =
            vec![Arc::new(ClientNode::new("aaa")), Arc::new(ClientNode::new("bbb"))];
        let router =
            ConsistentHashRouter::with_hash_function(&nodes, 1, Arc::new(FirstByte)).unwrap();
        // "a"(97) 与 "b"(98) 各占一个位置；"c"(99) 越过末尾 → 回绕到最小 hash 的节点
        assert_eq!(router.route_node("a!").unwrap().get_key(), "aaa");
        assert_eq!(router.route_node("b!").unwrap().get_key(), "bbb");
        assert_eq!(router.route_node("c!").unwrap().get_key(), "aaa");
    }
}
