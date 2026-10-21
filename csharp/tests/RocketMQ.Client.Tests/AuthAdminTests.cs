// Wire-level proof of the AUTH user-management and ACL-management admin RPCs (3001..3010).
//
// Everything these calls decide is inside the frame: which names ride in extFields, which keys
// the JSON body carries, and what a body that says nothing decodes to. A field that reached the
// broker as an empty string or as a null value is a different statement than one that stayed
// absent, and the broker reads only the former two as "set to nothing". So every claim here is
// read back off the frame the loopback broker recorded.
//
// Each positive assertion is paired with a control that pins the other side of the same rule: a
// key the caller left unset must stay off the body, a delete must not carry a policy-shaped
// header, a non-success code must not be reported as "no such user", and an address the caller
// named must not move when the fast-channel switch is on.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class AuthAdminTests
{
    private static DefaultMQAdminExt Started(MockCluster cluster, string tag, bool vipOn = false)
    {
        var admin = new DefaultMQAdminExt("ADMIN_auth_" + tag);
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.VipChannelEnabled = vipOn;
        admin.Start();
        return admin;
    }

    private static WireRecord Single(MockCluster cluster, int code) =>
        Assert.Single(cluster.Records().FindAll(r => r.Code == code));

    private static string BodyOf(WireRecord record) => Encoding.UTF8.GetString(record.Body);

    private static byte[] Bytes(string json) => Encoding.UTF8.GetBytes(json);

    private static List<string> KeysOf(WireRecord record) => new(record.Ext.Keys);

    // ---------------------------------------------------------------- user: write side

    [Fact]
    public void CreateUserObject_CarriesTheUsernameHeaderAndEverySetFieldOfTheBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "create_user_obj");
        try
        {
            admin.CreateUser(cluster.BrokerAddrs[0], new UserInfo
            {
                Username = "u_admin",
                Password = "s3cret",
                UserType = AuthUserType.Super,
                UserStatus = AuthUserStatus.Disable,
            });

            WireRecord rec = Single(cluster, RequestCode.AuthCreateUser);
            Assert.Equal(new[] { "username" }, KeysOf(rec));
            Assert.Equal("u_admin", rec.Ext["username"]);
            Assert.True(rec.HasBody);
            Assert.Equal(
                "{\"username\":\"u_admin\",\"password\":\"s3cret\",\"userType\":\"Super\","
                + "\"userStatus\":\"disable\"}",
                BodyOf(rec));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>The three-argument form states no status: the key is absent, not null.</summary>
    [Fact]
    public void CreateUserConvenience_LeavesTheUserStatusKeyOffTheBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "create_user_short");
        try
        {
            admin.CreateUser(cluster.BrokerAddrs[0], "u_dev", "pw-42", AuthUserType.Normal);

            WireRecord rec = Single(cluster, RequestCode.AuthCreateUser);
            string body = BodyOf(rec);
            Assert.Equal(
                "{\"username\":\"u_dev\",\"password\":\"pw-42\",\"userType\":\"Normal\"}", body);
            Assert.DoesNotContain("userStatus", body);
            Assert.DoesNotContain("null", body);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateUserObject_UsesItsOwnCodeWithTheSameShape()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "update_user_obj");
        try
        {
            admin.UpdateUser(cluster.BrokerAddrs[0], new UserInfo
            {
                Username = "u_ops",
                Password = "pw-7",
                UserType = AuthUserType.Normal,
                UserStatus = AuthUserStatus.Enable,
            });

            WireRecord rec = Single(cluster, RequestCode.AuthUpdateUser);
            Assert.Equal(new[] { "username" }, KeysOf(rec));
            Assert.Equal("u_ops", rec.Ext["username"]);
            Assert.Equal(
                "{\"username\":\"u_ops\",\"password\":\"pw-7\",\"userType\":\"Normal\","
                + "\"userStatus\":\"enable\"}",
                BodyOf(rec));
            Assert.Equal(0, cluster.CountRequests(RequestCode.AuthCreateUser));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateUserConvenience_LeavesTheUserStatusKeyOffTheBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "update_user_short");
        try
        {
            admin.UpdateUser(cluster.BrokerAddrs[0], "u_dev", "pw-43", AuthUserType.Super);

            WireRecord rec = Single(cluster, RequestCode.AuthUpdateUser);
            string body = BodyOf(rec);
            Assert.Equal(
                "{\"username\":\"u_dev\",\"password\":\"pw-43\",\"userType\":\"Super\"}", body);
            Assert.DoesNotContain("userStatus", body);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void DeleteUser_CarriesOnlyTheUsernameAndNoBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "delete_user");
        try
        {
            admin.DeleteUser(cluster.BrokerAddrs[0], "u_gone");

            WireRecord rec = Single(cluster, RequestCode.AuthDeleteUser);
            Assert.Equal(new[] { "username" }, KeysOf(rec));
            Assert.Equal("u_gone", rec.Ext["username"]);
            Assert.False(rec.HasBody);
            Assert.Empty(rec.Body);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- user: read side

    [Fact]
    public void GetUser_DecodesTheSingleUserObject()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyBody(RequestCode.AuthGetUser, _ => Bytes(
            "{\"username\":\"u_read\",\"password\":\"pw-read\",\"userType\":\"Super\","
            + "\"userStatus\":\"enable\"}"));
        DefaultMQAdminExt admin = Started(cluster, "get_user");
        try
        {
            UserInfo? user = admin.GetUser(cluster.BrokerAddrs[0], "u_read");

            Assert.NotNull(user);
            Assert.Equal("u_read", user!.Username);
            Assert.Equal("pw-read", user.Password);
            Assert.Equal(AuthUserType.Super, user.UserType);
            Assert.Equal(AuthUserStatus.Enable, user.UserStatus);
            Assert.Equal(new[] { "username" }, KeysOf(Single(cluster, RequestCode.AuthGetUser)));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetUser_EmptyBodyIsNoUser()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "get_user_empty");
        try
        {
            Assert.Null(admin.GetUser(cluster.BrokerAddrs[0], "u_missing"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>An array body is not one user: reporting it as a found-but-empty user would turn
    /// a wrong shape into a silent success.</summary>
    [Fact]
    public void GetUser_NonObjectBodyIsNoUser()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyBody(RequestCode.AuthGetUser, _ => Bytes("[{\"username\":\"u_x\"}]"));
        DefaultMQAdminExt admin = Started(cluster, "get_user_shape");
        try
        {
            Assert.Null(admin.GetUser(cluster.BrokerAddrs[0], "u_x"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ListUser_DecodesTheArrayAndKeepsAbsentFieldsUnset()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyBody(RequestCode.AuthListUser, _ => Bytes(
            "[{\"username\":\"u_one\",\"password\":\"p1\",\"userType\":\"Normal\","
            + "\"userStatus\":\"disable\"},{\"username\":\"u_two\"}]"));
        DefaultMQAdminExt admin = Started(cluster, "list_user");
        try
        {
            List<UserInfo> users = admin.ListUser(cluster.BrokerAddrs[0], "u_");

            WireRecord rec = Single(cluster, RequestCode.AuthListUser);
            Assert.Equal(new[] { "filter" }, KeysOf(rec));
            Assert.Equal("u_", rec.Ext["filter"]);

            Assert.Equal(2, users.Count);
            Assert.Equal("u_one", users[0].Username);
            Assert.Equal("p1", users[0].Password);
            Assert.Equal(AuthUserType.Normal, users[0].UserType);
            Assert.Equal(AuthUserStatus.Disable, users[0].UserStatus);
            Assert.Equal("u_two", users[1].Username);
            Assert.Null(users[1].Password);
            Assert.Null(users[1].UserType);
            Assert.Null(users[1].UserStatus);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ListUser_EmptyBodyIsNoUsers()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "list_user_empty");
        try
        {
            Assert.Empty(admin.ListUser(cluster.BrokerAddrs[0], "u_none"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>Nothing to filter on means the header says nothing: an empty value would be a
    /// different statement than an absent key.</summary>
    [Fact]
    public void ListUserWithoutFilterLeavesTheHeaderEmpty()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "list_user_nofilter");
        try
        {
            admin.ListUser(cluster.BrokerAddrs[0]);

            WireRecord rec = Single(cluster, RequestCode.AuthListUser);
            Assert.Empty(rec.Ext);
            Assert.False(rec.Ext.ContainsKey("filter"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- acl: write side

    [Fact]
    public void CreateAclObject_NestedBodyIsByteExact()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "create_acl_obj");
        try
        {
            admin.CreateAcl(cluster.BrokerAddrs[0], new AclInfo
            {
                Subject = "u:alice",
                Policies = new List<AclPolicy>
                {
                    new()
                    {
                        PolicyType = AuthPolicyType.Custom,
                        Entries = new List<AclEntry>
                        {
                            new()
                            {
                                Resource = "TopicA",
                                Actions = new List<string> { "Pub", "Sub" },
                                SourceIps = new List<string> { "10.0.0.1", "192.168.1.0/24" },
                                Decision = AuthDecision.Allow,
                            },
                        },
                    },
                    new()
                    {
                        PolicyType = AuthPolicyType.Default,
                        Entries = new List<AclEntry>
                        {
                            new()
                            {
                                Resource = "GroupB",
                                Actions = new List<string> { "Pub" },
                                SourceIps = new List<string> { "*" },
                                Decision = AuthDecision.Deny,
                            },
                        },
                    },
                },
            });

            WireRecord rec = Single(cluster, RequestCode.AuthCreateAcl);
            Assert.Equal(new[] { "subject" }, KeysOf(rec));
            Assert.Equal("u:alice", rec.Ext["subject"]);
            Assert.Equal(
                "{\"subject\":\"u:alice\",\"policies\":[{\"policyType\":\"Custom\",\"entries\":"
                + "[{\"resource\":\"TopicA\",\"actions\":[\"Pub\",\"Sub\"],\"sourceIps\":"
                + "[\"10.0.0.1\",\"192.168.1.0/24\"],\"decision\":\"Allow\"}]},"
                + "{\"policyType\":\"Default\",\"entries\":[{\"resource\":\"GroupB\","
                + "\"actions\":[\"Pub\"],\"sourceIps\":[\"*\"],\"decision\":\"Deny\"}]}]}",
                BodyOf(rec));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>A flat grant collapses into one policy with no type of its own, and every
    /// resource gets an entry repeating the same actions / sourceIps / decision.</summary>
    [Fact]
    public void CreateAclConvenience_BuildsOneUntypedPolicyWithOneEntryPerResource()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "create_acl_short");
        try
        {
            admin.CreateAcl(cluster.BrokerAddrs[0], "u:carol",
                new List<string> { "TopicA", "TopicB" },
                new List<string> { "Sub" },
                new List<string> { "10.1.2.3" },
                AuthDecision.Deny);

            WireRecord rec = Single(cluster, RequestCode.AuthCreateAcl);
            Assert.Equal(new[] { "subject" }, KeysOf(rec));
            string body = BodyOf(rec);
            Assert.Equal(
                "{\"subject\":\"u:carol\",\"policies\":[{\"entries\":[{"
                + "\"resource\":\"TopicA\",\"actions\":[\"Sub\"],\"sourceIps\":[\"10.1.2.3\"],"
                + "\"decision\":\"Deny\"},{\"resource\":\"TopicB\",\"actions\":[\"Sub\"],"
                + "\"sourceIps\":[\"10.1.2.3\"],\"decision\":\"Deny\"}]}]}",
                body);
            Assert.DoesNotContain("policyType", body);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateAclUsesItsOwnCodeInBothForms()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "update_acl");
        try
        {
            admin.UpdateAcl(cluster.BrokerAddrs[0], new AclInfo
            {
                Subject = "u:dave",
                Policies = new List<AclPolicy>
                {
                    new()
                    {
                        PolicyType = AuthPolicyType.Custom,
                        Entries = new List<AclEntry>
                        {
                            new()
                            {
                                Resource = "TopicC",
                                Actions = new List<string> { "Pub" },
                                SourceIps = new List<string> { "10.9.9.9" },
                                Decision = AuthDecision.Allow,
                            },
                        },
                    },
                },
            });
            admin.UpdateAcl(cluster.BrokerAddrs[0], "u:eve",
                new List<string> { "TopicD" }, new List<string> { "Sub" },
                new List<string> { "10.8.8.8" }, AuthDecision.Deny);

            List<WireRecord> recs = cluster.Records().FindAll(
                r => r.Code == RequestCode.AuthUpdateAcl);
            Assert.Equal(2, recs.Count);
            Assert.Equal(0, cluster.CountRequests(RequestCode.AuthCreateAcl));

            Assert.Equal(new[] { "subject" }, KeysOf(recs[0]));
            Assert.Equal("u:dave", recs[0].Ext["subject"]);
            Assert.Contains("\"policyType\":\"Custom\"", BodyOf(recs[0]));

            Assert.Equal(new[] { "subject" }, KeysOf(recs[1]));
            Assert.Equal("u:eve", recs[1].Ext["subject"]);
            Assert.DoesNotContain("policyType", BodyOf(recs[1]));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void DeleteAcl_HeaderIsExactlySubjectAndResource()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "delete_acl");
        try
        {
            admin.DeleteAcl(cluster.BrokerAddrs[0], "u:dave", "TopicZ");

            WireRecord rec = Single(cluster, RequestCode.AuthDeleteAcl);
            Assert.Equal(new[] { "resource", "subject" }, KeysOf(rec));
            Assert.Equal("u:dave", rec.Ext["subject"]);
            Assert.Equal("TopicZ", rec.Ext["resource"]);
            // nothing policy-shaped rides along on a delete
            Assert.False(rec.Ext.ContainsKey("policyType"));
            Assert.False(rec.Ext.ContainsKey("policies"));
            Assert.False(rec.Ext.ContainsKey("decision"));
            Assert.False(rec.HasBody);
            Assert.Empty(rec.Body);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- acl: read side

    [Fact]
    public void GetAcl_DecodesTheSingleAclObject()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyBody(RequestCode.AuthGetAcl, _ => Bytes(
            "{\"subject\":\"u:alice\",\"policies\":[{\"policyType\":\"Custom\",\"entries\":[{"
            + "\"resource\":\"TopicA\",\"actions\":[\"Pub\",\"Sub\"],"
            + "\"sourceIps\":[\"10.0.0.1\"],\"decision\":\"Allow\"}]}]}"));
        DefaultMQAdminExt admin = Started(cluster, "get_acl");
        try
        {
            AclInfo? acl = admin.GetAcl(cluster.BrokerAddrs[0], "u:alice");

            Assert.NotNull(acl);
            Assert.Equal("u:alice", acl!.Subject);
            Assert.NotNull(acl.Policies);
            Assert.Single(acl.Policies!);
            Assert.Equal(AuthPolicyType.Custom, acl.Policies![0].PolicyType);
            AclEntry entry = Assert.Single(acl.Policies![0].Entries!);
            Assert.Equal("TopicA", entry.Resource);
            Assert.Equal(new List<string> { "Pub", "Sub" }, entry.Actions);
            Assert.Equal(new List<string> { "10.0.0.1" }, entry.SourceIps);
            Assert.Equal(AuthDecision.Allow, entry.Decision);
            Assert.Equal(new[] { "subject" }, KeysOf(Single(cluster, RequestCode.AuthGetAcl)));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetAcl_EmptyBodyIsNoAcl()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "get_acl_empty");
        try
        {
            Assert.Null(admin.GetAcl(cluster.BrokerAddrs[0], "u:none"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ListAcl_DecodesTheArrayAndKeepsAbsentFieldsUnset()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyBody(RequestCode.AuthListAcl, _ => Bytes(
            "[{\"subject\":\"u:one\",\"policies\":[{\"entries\":[{"
            + "\"resource\":\"TopicA\",\"actions\":[\"Pub\"],\"sourceIps\":[\"*\"],"
            + "\"decision\":\"Allow\"}]}]},{\"subject\":\"u:two\"}]"));
        DefaultMQAdminExt admin = Started(cluster, "list_acl");
        try
        {
            List<AclInfo> acls = admin.ListAcl(cluster.BrokerAddrs[0], "u:", "Topic");

            WireRecord rec = Single(cluster, RequestCode.AuthListAcl);
            Assert.Equal(new[] { "resourceFilter", "subjectFilter" }, KeysOf(rec));
            Assert.Equal("u:", rec.Ext["subjectFilter"]);
            Assert.Equal("Topic", rec.Ext["resourceFilter"]);

            Assert.Equal(2, acls.Count);
            Assert.Equal("u:one", acls[0].Subject);
            AclPolicy policy = Assert.Single(acls[0].Policies!);
            Assert.Null(policy.PolicyType);
            AclEntry entry = Assert.Single(policy.Entries!);
            Assert.Equal("TopicA", entry.Resource);
            Assert.Equal("u:two", acls[1].Subject);
            Assert.Null(acls[1].Policies);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ListAcl_EmptyBodyIsNoAcls()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "list_acl_empty");
        try
        {
            Assert.Empty(admin.ListAcl(cluster.BrokerAddrs[0], "u_none", "T_none"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ListAclWithoutFiltersLeavesTheHeaderEmpty()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "list_acl_nofilter");
        try
        {
            admin.ListAcl(cluster.BrokerAddrs[0]);

            WireRecord rec = Single(cluster, RequestCode.AuthListAcl);
            Assert.Empty(rec.Ext);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- response codes

    [Fact]
    public void WriteRejectedByTheBrokerThrowsWithItsCodeAndRemark()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.AuthCreateUser, ResponseCode.NoPermission);
        DefaultMQAdminExt admin = Started(cluster, "write_error");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.CreateUser(cluster.BrokerAddrs[0], "u_x", "p_x", AuthUserType.Normal));
            Assert.Equal(ResponseCode.NoPermission, e.ResponseCode);
            Assert.Equal("mock failure", e.ResponseMessage);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>A rejection is not "no such user": the read side has to surface the code rather
    /// than fall through to the empty-body answer.</summary>
    [Fact]
    public void ReadRejectedByTheBrokerThrowsInsteadOfReportingNoUser()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.AuthGetUser, ResponseCode.SystemError);
        cluster.SetReplyCode(RequestCode.AuthListAcl, ResponseCode.SystemError);
        DefaultMQAdminExt admin = Started(cluster, "read_error");
        try
        {
            Assert.Throws<MQBrokerException>(() => admin.GetUser(cluster.BrokerAddrs[0], "u_x"));
            Assert.Throws<MQBrokerException>(() =>
                admin.ListAcl(cluster.BrokerAddrs[0], "u", "T"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- addressing

    /// <summary>The caller's address is the destination: nothing here is re-resolved, and the
    /// fast-channel rewrite the other admin reads perform must not move these requests.</summary>
    [Fact]
    public void AuthRequestsStayOnTheAddressTheCallerNamed()
    {
        using var cluster = MockCluster.Start(1);
        string addr = cluster.BrokerAddrs[0];
        DefaultMQAdminExt admin = Started(cluster, "address", vipOn: true);
        try
        {
            admin.DeleteUser(addr, "u_vip");
            admin.CreateUser(addr, "u_vip", "p_vip", AuthUserType.Normal);
            admin.DeleteAcl(addr, "u_vip", "TopicVip");

            Assert.Equal(1, cluster.CountRequests(RequestCode.AuthDeleteUser));
            Assert.Equal(1, cluster.CountRequests(RequestCode.AuthCreateUser));
            Assert.Equal(1, cluster.CountRequests(RequestCode.AuthDeleteAcl));

            // control: the address a fast-channel rewrite would have used really is a different
            // endpoint, so an arrival here can only mean the address went through untouched
            Assert.NotEqual(addr, MixAll.BrokerVipChannel(true, addr));
            Assert.Equal(0, cluster.CountRequests(RequestCode.GetBrokerRuntimeInfo));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>Same control seen from the other side: a read that does honour the switch never
    /// reaches this listener, while an auth call on the same admin does.</summary>
    [Fact]
    public void AMovedAdminReadNeverReachesTheListenerTheAuthCallDoes()
    {
        using var cluster = MockCluster.Start(1);
        string addr = cluster.BrokerAddrs[0];
        DefaultMQAdminExt admin = Started(cluster, "address_control", vipOn: true);
        try
        {
            try
            {
                admin.FetchBrokerRuntimeStats(addr, 1000);
            }
            catch (Exception)
            {
                // the fast-channel endpoint of this mock broker is not listening
            }

            admin.GetUser(addr, "u_here");

            Assert.Equal(0, cluster.CountRequests(RequestCode.GetBrokerRuntimeInfo));
            Assert.Equal(1, cluster.CountRequests(RequestCode.AuthGetUser));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- model level

    /// <summary>The exact wire spellings, including the deliberate inconsistency between the
    /// capitalization of a type and that of a status.</summary>
    [Fact]
    public void EnumWireSpellingsAreByteExact()
    {
        Assert.Equal("Super", AuthUserType.Super);
        Assert.Equal("Normal", AuthUserType.Normal);
        Assert.Equal("enable", AuthUserStatus.Enable);
        Assert.Equal("disable", AuthUserStatus.Disable);
        Assert.Equal("Custom", AuthPolicyType.Custom);
        Assert.Equal("Default", AuthPolicyType.Default);
        Assert.Equal("Allow", AuthDecision.Allow);
        Assert.Equal("Deny", AuthDecision.Deny);

        Assert.Equal("super", AuthUserType.Super.ToLowerInvariant());
        Assert.NotEqual("enable", AuthUserStatus.Disable);
    }

    [Fact]
    public void UserInfo_EncodeWritesOnlyWhatIsSet()
    {
        Assert.Equal("{}", Encoding.UTF8.GetString(new UserInfo().Encode()));
        string partial = Encoding.UTF8.GetString(new UserInfo { Username = "u_a" }.Encode());
        Assert.Equal("{\"username\":\"u_a\"}", partial);
        Assert.DoesNotContain("null", partial);
    }

    [Fact]
    public void AclEntry_DistinguishesAnUnsetListFromAnEmptyOne()
    {
        Assert.Equal("{}", new AclEntry().ToJson().Dump());
        Assert.Equal("{\"actions\":[]}",
            new AclEntry { Actions = new List<string>() }.ToJson().Dump());
        Assert.Equal("{\"resource\":\"TopicA\"}",
            new AclEntry { Resource = "TopicA" }.ToJson().Dump());
    }

    [Fact]
    public void AclInfo_RoundTripsThroughItsEncodedForm()
    {
        AclInfo acl = AclInfo.OfResources("u:round", new List<string> { "TopicA", "TopicB" },
            new List<string> { "Pub", "Sub" }, new List<string> { "1.2.3.4" }, AuthDecision.Allow);
        byte[] wire = acl.Encode();

        Assert.True(AclInfo.Decode(wire, out AclInfo back));
        Assert.Equal("u:round", back.Subject);
        AclPolicy policy = Assert.Single(back.Policies!);
        Assert.Null(policy.PolicyType);
        Assert.Equal(2, policy.Entries!.Count);
        Assert.Equal("TopicB", policy.Entries![1].Resource);
        Assert.Equal(new List<string> { "Pub", "Sub" }, policy.Entries![1].Actions);
        Assert.Equal(new List<string> { "1.2.3.4" }, policy.Entries![1].SourceIps);
        Assert.Equal(AuthDecision.Allow, policy.Entries![1].Decision);

        // an object body never decodes as a list, and a list body never decodes as one object
        Assert.Empty(UserInfo.DecodeList(wire));
        Assert.False(UserInfo.Decode(Array.Empty<byte>(), out _));
        Assert.False(UserInfo.Decode(Bytes("[]"), out _));
        Assert.Empty(UserInfo.DecodeList(Bytes("not json")));
    }
}
