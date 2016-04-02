package com.rethinkdb;

import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlQueryLogicError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.model.OptArgs;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;
import net.jodah.concurrentunit.Waiter;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AuthTest {
    public static final RethinkDB r = RethinkDB.r;
    static final String dbName = "javatests";
    static final String tableName = "atest";
    static final String bogusUsername = "bogus_guy";
    static final String bogusPassword = "bogus_man+=,";

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void oneTimeSetUp() throws Exception {
        Connection adminConn = TestingFramework.createConnection();
        r.db("rethinkdb").table("users").insert(
                r.hashMap("id", bogusUsername)
                        .with("password", bogusPassword))
                .run(adminConn);
        adminConn.close();
    }

    @AfterClass
    public static void oneTimeTearDown() throws Exception {
        Connection adminConn = TestingFramework.createConnection();
        r.db("rethinkdb").table("users").get(bogusUsername).delete();
        adminConn.close();
    }

    @Test
    public void testConnectWithNonAdminUser() throws Exception {
        Connection bogusConn = TestingFramework.defaultConnectionBuilder()
                .user(bogusUsername, bogusPassword).connect();
        bogusConn.close();
    }
}
