package gaillard.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import org.bson.types.ObjectId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.Before;

public class QueueTest {

    private DBCollection collection;
    private Queue queue;

    @Before
    public void setup() throws UnknownHostException {
        collection = new MongoClient().getDB("testing").getCollection("messages");
        collection.drop();

        queue = new Queue(collection);
    }

    @Test(expected = NullPointerException.class)
    public void construct_nullCollection() {
        new Queue(null);
    }

    @Test
    public void ensureGetIndex() {
        queue.ensureGetIndex(new BasicDBObject("type", 1).append("boo", -1));
        queue.ensureGetIndex(new BasicDBObject("another.sub", 1));

        final List<DBObject> indexInfo = collection.getIndexInfo();

        assertEquals(4, indexInfo.size());

        final BasicDBObject expectedOne = new BasicDBObject("running", 1)
                .append("payload.type", 1)
                .append("priority", 1)
                .append("created", 1)
                .append("payload.boo", -1)
                .append("earliestGet", 1);
        assertEquals(expectedOne, indexInfo.get(1).get("key"));

        final BasicDBObject expectedTwo = new BasicDBObject("running", 1).append("resetTimestamp", 1);
        assertEquals(expectedTwo, indexInfo.get(2).get("key"));

        final BasicDBObject expectedThree = new BasicDBObject("running", 1)
                .append("payload.another.sub", 1)
                .append("priority", 1)
                .append("created", 1)
                .append("earliestGet", 1);
        assertEquals(expectedThree, indexInfo.get(3).get("key"));
    }

    @Test
    public void ensureGetIndex_noArgs() {
        queue.ensureGetIndex();

        final List<DBObject> indexInfo = collection.getIndexInfo();

        assertEquals(3, indexInfo.size());

        final BasicDBObject expectedOne = new BasicDBObject("running", 1).append("priority", 1).append("created", 1).append("earliestGet", 1);
        assertEquals(expectedOne, indexInfo.get(1).get("key"));

        final BasicDBObject expectedTwo = new BasicDBObject("running", 1).append("resetTimestamp", 1);
        assertEquals(expectedTwo, indexInfo.get(2).get("key"));
    }

    @Test(expected = RuntimeException.class)
    public void ensureGetIndex_tooLongCollectionName() throws UnknownHostException {
        //121 chars
        final String collectionName = "messages01234567890123456789012345678901234567890123456789"
                + "012345678901234567890123456789012345678901234567890123456789012";

        collection = new MongoClient().getDB("testing").getCollection(collectionName);
        queue = new Queue(collection);
        queue.ensureGetIndex();
    }

    @Test(expected = IllegalArgumentException.class)
    public void ensureGetIndex_badBeforeSortValue() {
        queue.ensureGetIndex(new BasicDBObject("field", "NotAnInt"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void ensureGetIndex_badAfterSortValue() {
        queue.ensureGetIndex(new BasicDBObject(), new BasicDBObject("field", "NotAnInt"));
    }

    @Test(expected = NullPointerException.class)
    public void ensureGetIndex_nullBeforeSort() {
        queue.ensureGetIndex(null);
    }

    @Test(expected = NullPointerException.class)
    public void ensureGetIndex_nullAfterSort() {
        queue.ensureGetIndex(new BasicDBObject(), null);
    }

    @Test
    public void ensureCountIndex() {
        queue.ensureCountIndex(new BasicDBObject("type", 1).append("boo", -1), false);
        queue.ensureCountIndex(new BasicDBObject("another.sub", 1), true);

        final List<DBObject> indexInfo = collection.getIndexInfo();

        assertEquals(3, indexInfo.size());

        final BasicDBObject expectedOne = new BasicDBObject("payload.type", 1).append("payload.boo", -1);
        assertEquals(expectedOne, indexInfo.get(1).get("key"));

        final BasicDBObject expectedTwo = new BasicDBObject("running", 1).append("payload.another.sub", 1);
        assertEquals(expectedTwo, indexInfo.get(2).get("key"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void ensureCountIndex_badValue() {
        queue.ensureCountIndex(new BasicDBObject("field", "NotAnInt"), true);
    }

    @Test(expected = NullPointerException.class)
    public void ensureCountIndex_null() {
        queue.ensureCountIndex(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void get_nullQuery() {
        queue.get(null, Integer.MAX_VALUE);
    }

    @Test
    public void get_badQuery() {
        queue.send(new BasicDBObject("key", 0));

        assertNull(queue.get(new BasicDBObject("key", 1), Integer.MAX_VALUE, 0));
    }

    @Test
    public void get_fullQuery() {
        final BasicDBObject message = new BasicDBObject("id", "ID SHOULD BE REMOVED").append("key1", 0).append("key2", true);

        queue.send(message);
        queue.send(new BasicDBObject());

        final BasicDBObject result = queue.get(message, Integer.MAX_VALUE);
        assertNotEquals(message.get("id"), result.get("id"));

        message.put("id", result.get("id"));
        assertEquals(message, result);
    }

    @Test
    public void get_subQuery() {
        final BasicDBObject messageOne = new BasicDBObject("one", new BasicDBObject("two", new BasicDBObject("three", 5)));
        final BasicDBObject messageTwo = new BasicDBObject("one", new BasicDBObject("two", new BasicDBObject("three", 4)));

        queue.send(messageOne);
        queue.send(messageTwo);

        final BasicDBObject result = queue.get(new BasicDBObject("one.two.three", new BasicDBObject("$gte", 5)), Integer.MAX_VALUE);

        messageOne.put("id", result.get("id"));
        assertEquals(messageOne, result);
    }

    @Test
    public void get_negativeWait() {
        assertNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE, Integer.MIN_VALUE));

        queue.send(new BasicDBObject());

        assertNotNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE, Integer.MIN_VALUE));
    }

    @Test
    public void get_negativePoll() {
        assertNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE, 100, Long.MIN_VALUE));

        queue.send(new BasicDBObject());

        assertNotNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE, 100, Long.MIN_VALUE));
    }

    @Test
    public void get_beforeAck() {
        queue.send(new BasicDBObject());

        assertNotNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE));

        //try get message we already have before ack
        assertNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE, 0));
    }

    @Test
    public void get_customPriority() {
        final BasicDBObject messageOne = new BasicDBObject("key", 1);
        final BasicDBObject messageTwo = new BasicDBObject("key", 2);
        final BasicDBObject messageThree = new BasicDBObject("key", 3);

        queue.send(messageOne, new Date(), 0.5);
        queue.send(messageTwo, new Date(), 0.4);
        queue.send(messageThree, new Date(), 0.3);

        final BasicDBObject resultOne = queue.get(new BasicDBObject(), Integer.MAX_VALUE);
        final BasicDBObject resultTwo = queue.get(new BasicDBObject(), Integer.MAX_VALUE);
        final BasicDBObject resultThree = queue.get(new BasicDBObject(), Integer.MAX_VALUE);

        assertEquals(messageOne.get("key"), resultThree.get("key"));
        assertEquals(messageTwo.get("key"), resultTwo.get("key"));
        assertEquals(messageThree.get("key"), resultOne.get("key"));
    }

    @Test
    public void get_timePriority() {
        final BasicDBObject messageOne = new BasicDBObject("key", 1);
        final BasicDBObject messageTwo = new BasicDBObject("key", 2);
        final BasicDBObject messageThree = new BasicDBObject("key", 3);

        queue.send(messageOne, new Date());
        queue.send(messageTwo, new Date());
        queue.send(messageThree, new Date());

        final BasicDBObject resultOne = queue.get(new BasicDBObject(), Integer.MAX_VALUE);
        final BasicDBObject resultTwo = queue.get(new BasicDBObject(), Integer.MAX_VALUE);
        final BasicDBObject resultThree = queue.get(new BasicDBObject(), Integer.MAX_VALUE);

        assertEquals(messageOne.get("key"), resultOne.get("key"));
        assertEquals(messageTwo.get("key"), resultTwo.get("key"));
        assertEquals(messageThree.get("key"), resultThree.get("key"));
    }

    @Test
    public void get_wait() {
        final Date start = new Date();

        queue.get(new BasicDBObject(), Integer.MAX_VALUE, 200);

        final long elapsed = new Date().getTime() - start.getTime();

        assertTrue(elapsed >= 200);
        assertTrue(elapsed < 400);
    }

    @Test
    public void get_waitWhenMessageExists() {
        final Date start = new Date();

        queue.send(new BasicDBObject());

        queue.get(new BasicDBObject(), Integer.MAX_VALUE, 3000);

        assertTrue(new Date().getTime() - start.getTime() < 2000);
    }

    @Test
    public void get_earliestGet() throws InterruptedException {
        queue.send(new BasicDBObject(), new Date(System.currentTimeMillis() + 200));

        assertNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE, 0));

        Thread.sleep(200);

        assertNotNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE));
    }

    @Test
    public void get_resetStuck() {
        queue.send(new BasicDBObject());

        //sets resetTimestamp on messageOne
        assertNotNull(queue.get(new BasicDBObject(), 0));

        assertNotNull(queue.get(new BasicDBObject(), Integer.MAX_VALUE));
    }

    @Test
    public void count_running() {
        assertEquals(0, queue.count(new BasicDBObject(), true));
        assertEquals(0, queue.count(new BasicDBObject(), false));
        assertEquals(0, queue.count(new BasicDBObject()));

        queue.send(new BasicDBObject("key", 1));

        assertEquals(0, queue.count(new BasicDBObject(), true));
        assertEquals(1, queue.count(new BasicDBObject(), false));
        assertEquals(1, queue.count(new BasicDBObject()));

        queue.get(new BasicDBObject(), Integer.MAX_VALUE);

        assertEquals(1, queue.count(new BasicDBObject(), true));
        assertEquals(0, queue.count(new BasicDBObject(), false));
        assertEquals(1, queue.count(new BasicDBObject()));
    }

    @Test
    public void count_fullQuery() {
        final BasicDBObject message = new BasicDBObject("key", 1);

        queue.send(new BasicDBObject());
        queue.send(message);

        assertEquals(1, queue.count(message));
    }

    @Test
    public void count_subQuery() {
        final BasicDBObject messageOne = new BasicDBObject("one", new BasicDBObject("two", new BasicDBObject("three", 4)));
        final BasicDBObject messageTwo = new BasicDBObject("one", new BasicDBObject("two", new BasicDBObject("three", 5)));

        queue.send(messageOne);
        queue.send(messageTwo);

        assertEquals(1, queue.count(new BasicDBObject("one.two.three", new BasicDBObject("$gte", 5))));
    }

    @Test
    public void count_badQuery() {
        queue.send(new BasicDBObject("key", 0));

        assertEquals(0, queue.count(new BasicDBObject("key", 1)));
    }

    @Test(expected = NullPointerException.class)
    public void count_nullQuery() {
        queue.count(null);
    }

    @Test(expected = NullPointerException.class)
    public void count_runningNullQuery() {
        queue.count(null, true);
    }

    @Test
    public void ack() {
        final BasicDBObject message = new BasicDBObject("key", 0);

        queue.send(message);
        queue.send(new BasicDBObject());

        final BasicDBObject result = queue.get(message, Integer.MAX_VALUE);
        assertEquals(2, collection.count());

        queue.ack(result);
        assertEquals(1, collection.count());
    }

    @Test(expected = IllegalArgumentException.class)
    public void ack_wrongIdType() {
        queue.ack(new BasicDBObject("id", false));
    }

    @Test(expected = NullPointerException.class)
    public void ack_null() {
        queue.ack(null);
    }

    @Test
    public void ackSend() {
        final BasicDBObject message = new BasicDBObject("key", 0);

        queue.send(message);

        final BasicDBObject resultOne = queue.get(message, Integer.MAX_VALUE);

        final Date expectedEarliestGet = new Date();
        final double expectedPriority = 0.8;
        final Date timeBeforeAckSend = new Date();
        queue.ackSend(resultOne, new BasicDBObject("key", 1), expectedEarliestGet, expectedPriority);

        assertEquals(1, collection.count());

        final BasicDBObject actual = (BasicDBObject)collection.findOne();

        final Date actualCreated = actual.getDate("created");
        assertTrue(actualCreated.compareTo(timeBeforeAckSend) >= 0 && actualCreated.compareTo(new Date()) <= 0);

        final BasicDBObject expected = new BasicDBObject("_id", resultOne.get("id"))
                .append("payload", new BasicDBObject("key", 1))
                .append("running", false)
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", expectedEarliestGet)
                .append("priority", expectedPriority)
                .append("created", actual.get("created"));

        assertEquals(expected, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void ackSend_wrongIdType() {
        queue.ackSend(new BasicDBObject("id", 5), new BasicDBObject());
    }

    @Test(expected = IllegalArgumentException.class)
    public void ackSend_nanPriority() {
        queue.ackSend(new BasicDBObject("id", ObjectId.get()), new BasicDBObject(), new Date(), Double.NaN);
    }

    @Test(expected = NullPointerException.class)
    public void ackSend_nullMessage() {
        queue.ackSend(null, new BasicDBObject());
    }

    @Test(expected = NullPointerException.class)
    public void ackSend_nullPayload() {
        queue.ackSend(new BasicDBObject("id", ObjectId.get()), null);
    }

    @Test(expected = NullPointerException.class)
    public void ackSend_nullEarliestGet() {
        queue.ackSend(new BasicDBObject("id", ObjectId.get()), new BasicDBObject(), null);
    }

    @Test
    public void requeue() {
        final BasicDBObject message = new BasicDBObject("key", 0);

        queue.send(message);

        final BasicDBObject resultOne = queue.get(message, Integer.MAX_VALUE);

        final Date expectedEarliestGet = new Date();
        final double expectedPriority = 0.8;
        final Date timeBeforeRequeue = new Date();
        queue.requeue(resultOne, expectedEarliestGet, expectedPriority);

        assertEquals(1, collection.count());

        final BasicDBObject actual = (BasicDBObject)collection.findOne();

        final Date actualCreated = actual.getDate("created");
        assertTrue(actualCreated.compareTo(timeBeforeRequeue) >= 0 && actualCreated.compareTo(new Date()) <= 0);

        final BasicDBObject expected = new BasicDBObject("_id", resultOne.get("id"))
                .append("payload", new BasicDBObject("key", 0))
                .append("running", false)
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", expectedEarliestGet)
                .append("priority", expectedPriority)
                .append("created", actual.get("created"));

        assertEquals(expected, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void requeue_wrongIdType() {
        queue.requeue(new BasicDBObject("id", new BasicDBObject()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void requeue_nanPriority() {
        queue.requeue(new BasicDBObject("id", ObjectId.get()), new Date(), Double.NaN);
    }

    @Test(expected = NullPointerException.class)
    public void requeue_nullMessage() {
        queue.requeue(null);
    }

    @Test(expected = NullPointerException.class)
    public void requeue_nullEarliestGet() {
        queue.requeue(new BasicDBObject("id", ObjectId.get()), null);
    }

    @Test
    public void send() {
        final BasicDBObject message = new BasicDBObject("key", 0);

        final Date expectedEarliestGet = new Date();
        final double expectedPriority = 0.8;
        final Date timeBeforeSend = new Date();
        queue.send(message, expectedEarliestGet, expectedPriority);

        assertEquals(1, collection.count());

        final BasicDBObject actual = (BasicDBObject)collection.findOne();

        final Date actualCreated = actual.getDate("created");
        assertTrue(actualCreated.compareTo(timeBeforeSend) >= 0 && actualCreated.compareTo(new Date()) <= 0);

        final BasicDBObject expected = new BasicDBObject("_id", actual.get("_id"))
                .append("payload", new BasicDBObject("key", 0))
                .append("running", false)
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", expectedEarliestGet)
                .append("priority", expectedPriority)
                .append("created", actual.get("created"));

        assertEquals(expected, actual);
    }

    @Test(expected = IllegalArgumentException.class)
    public void send_nanPriority() {
        queue.send(new BasicDBObject("id", ObjectId.get()), new Date(), Double.NaN);
    }

    @Test(expected = NullPointerException.class)
    public void send_nullMessage() {
        queue.send(null);
    }

    @Test(expected = NullPointerException.class)
    public void send_nullEarliestGet() {
        queue.send(new BasicDBObject("id", ObjectId.get()), null);
    }
}
