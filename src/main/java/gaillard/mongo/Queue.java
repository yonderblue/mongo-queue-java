package gaillard.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.CommandFailureException;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import java.util.Calendar;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import org.bson.types.ObjectId;

public final class Queue {

    private final DBCollection collection;

    public Queue(final DBCollection collection) {
        Objects.requireNonNull(collection);

        this.collection = collection;
    }

    /**
     * Ensure index for get() method with no fields before or after sort fields
     */
    public void ensureGetIndex() {
        ensureGetIndex(new BasicDBObject());
    }

    /**
     * Ensure index for get() method with no fields after sort fields
     *
     * @param beforeSort fields in get() call that should be before the sort fields in the index. Should not be null
     */
    public void ensureGetIndex(final BasicDBObject beforeSort) {
        ensureGetIndex(beforeSort, new BasicDBObject());
    }

    /**
     * Ensure index for get() method
     *
     * @param beforeSort fields in get() call that should be before the sort fields in the index. Should not be null
     * @param afterSort fields in get() call that should be after the sort fields in the index. Should not be null
     */
    public void ensureGetIndex(final BasicDBObject beforeSort, final BasicDBObject afterSort) {
        Objects.requireNonNull(beforeSort);
        Objects.requireNonNull(afterSort);

        //using general rule: equality, sort, range or more equality tests in that order for index
        final BasicDBObject completeIndex = new BasicDBObject("running", 1);

        for (final Entry<String, Object> field : beforeSort.entrySet()) {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }

            completeIndex.append("payload." + field.getKey(), field.getValue());
        }

        completeIndex.append("priority", 1).append("created", 1);

        for (final Entry<String, Object> field : afterSort.entrySet()) {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }

            completeIndex.append("payload." + field.getKey(), field.getValue());
        }

        completeIndex.append("earliestGet", 1);

        ensureIndex(completeIndex);//main query in Get()
        ensureIndex(new BasicDBObject("running", 1).append("resetTimestamp", 1));//for the stuck messages query in Get()
    }

    /**
     * Ensure index for count() method
     *
     * @param index fields in count() call. Should not be null
     * @param includeRunning whether running was given to count() or not
     */
    public void ensureCountIndex(final BasicDBObject index, final boolean includeRunning) {
        Objects.requireNonNull(index);

        final BasicDBObject completeIndex = new BasicDBObject();

        if (includeRunning) {
            completeIndex.append("running", 1);
        }

        for (final Entry<String, Object> field : index.entrySet()) {
            if (!Objects.equals(field.getValue(), 1) && !Objects.equals(field.getValue(), -1)) {
                throw new IllegalArgumentException("field values must be either 1 or -1");
            }

            completeIndex.append("payload." + field.getKey(), field.getValue());
        }

        ensureIndex(completeIndex);
    }

    /**
     * Get a non running message from queue with a wait of 3 seconds and poll of 200 milliseconds
     *
     * @param query query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3},
     * invalid {$and: [{...}, {...}]}. Should not be null.
     * @param resetDuration duration in seconds before this message is considered abandoned and will be given with another call to get()
     * @return message or null
     */
    public BasicDBObject get(final BasicDBObject query, final int resetDuration) {
        return get(query, resetDuration, 3000, 200);
    }

    /**
     * Get a non running message from queue with a poll of 200 milliseconds
     *
     * @param query query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3},
     * invalid {$and: [{...}, {...}]}. Should not be null.
     * @param resetDuration duration in seconds before this message is considered abandoned and will be given with another call to get()
     * @param waitDuration duration in milliseconds to keep polling before returning null
     * @return message or null
     */
    public BasicDBObject get(final BasicDBObject query, final int resetDuration, final int waitDuration) {
        return get(query, resetDuration, waitDuration, 200);
    }

    /**
     * Get a non running message from queue
     *
     * @param query query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3},
     * invalid {$and: [{...}, {...}]}. Should not be null.
     * @param resetDuration duration in seconds before this message is considered abandoned and will be given with another call to get()
     * @param waitDuration duration in milliseconds to keep polling before returning null
     * @param pollDuration duration in milliseconds between poll attempts
     * @return message or null
     */
    public BasicDBObject get(final BasicDBObject query, final int resetDuration, final int waitDuration, long pollDuration) {
        Objects.requireNonNull(query);

        //reset stuck messages
        collection.update(new BasicDBObject("running", true).append("resetTimestamp", new BasicDBObject("$lte", new Date())),
                new BasicDBObject("$set", new BasicDBObject("running", false)),
                false,
                true);

        final BasicDBObject builtQuery = new BasicDBObject("running", false);
        for (final Entry<String, Object> field : query.entrySet()) {
            builtQuery.append("payload." + field.getKey(), field.getValue());
        }

        builtQuery.append("earliestGet", new BasicDBObject("$lte", new Date()));

        final Calendar calendar = Calendar.getInstance();

        calendar.add(Calendar.SECOND, resetDuration);
        final Date resetTimestamp = calendar.getTime();

        final BasicDBObject sort = new BasicDBObject("priority", 1).append("created", 1);
        final BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("running", true).append("resetTimestamp", resetTimestamp));
        final BasicDBObject fields = new BasicDBObject("payload", 1);

        calendar.setTimeInMillis(System.currentTimeMillis());
        calendar.add(Calendar.MILLISECOND, waitDuration);
        final Date end = calendar.getTime();

        while (true) {
            final BasicDBObject message = (BasicDBObject) collection.findAndModify(builtQuery, fields, sort, false, update, true, false);
            if (message != null) {
                final ObjectId id = message.getObjectId("_id");
                return ((BasicDBObject) message.get("payload")).append("id", id);
            }

            if (new Date().compareTo(end) >= 0) {
                return null;
            }

            try {
                Thread.sleep(pollDuration);
            } catch (final InterruptedException ex) {
                throw new RuntimeException(ex);
            } catch (final IllegalArgumentException ex) {
                pollDuration = 0;
            }
        }
    }

    /**
     * Count in queue, running true or false
     *
     * @param query query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3},
     * invalid {$and: [{...}, {...}]}. Should not be null
     * @return count
     */
    public long count(final BasicDBObject query) {
        Objects.requireNonNull(query);

        final BasicDBObject completeQuery = new BasicDBObject();

        for (final Entry<String, Object> field : query.entrySet()) {
            completeQuery.append("payload." + field.getKey(), field.getValue());
        }

        return collection.count(completeQuery);
    }

    /**
     * Count in queue
     *
     * @param query query where top level fields do not contain operators. Lower level fields can however. eg: valid {a: {$gt: 1}, "b.c": 3},
     * invalid {$and: [{...}, {...}]}. Should not be null
     * @param running count running messages or not running
     * @return count
     */
    public long count(final BasicDBObject query, final boolean running) {
        Objects.requireNonNull(query);

        final BasicDBObject completeQuery = new BasicDBObject("running", running);

        for (final Entry<String, Object> field : query.entrySet()) {
            completeQuery.append("payload." + field.getKey(), field.getValue());
        }

        return collection.count(completeQuery);
    }

    /**
     * Acknowledge a message was processed and remove from queue
     *
     * @param message message received from get(). Should not be null.
     */
    public void ack(final BasicDBObject message) {
        Objects.requireNonNull(message);
        final Object id = message.get("id");
        if (id.getClass() != ObjectId.class) {
            throw new IllegalArgumentException("id must be an ObjectId");
        }

        collection.remove(new BasicDBObject("_id", id));
    }

    /**
     * Ack message and send payload to queue, atomically, with earliestGet as Now and 0.0 priority
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     */
    public void ackSend(final BasicDBObject message, final BasicDBObject payload) {
        ackSend(message, payload, new Date());
    }

    /**
     * Ack message and send payload to queue, atomically, with 0.0 priority
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     */
    public void ackSend(final BasicDBObject message, final BasicDBObject payload, final Date earliestGet) {
        ackSend(message, payload, earliestGet, 0.0);
    }

    /**
     * Ack message and send payload to queue, atomically
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     * @param priority priority for order out of get(). 0 is higher priority than 1. Should not be NaN
     */
    public void ackSend(final BasicDBObject message, final BasicDBObject payload, final Date earliestGet, final double priority) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }

        final Object id = message.get("id");
        if (id.getClass() != ObjectId.class) {
            throw new IllegalArgumentException("id must be an ObjectId");
        }

        final BasicDBObject newMessage = new BasicDBObject("payload", payload)
                .append("running", false)
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", earliestGet)
                .append("priority", priority)
                .append("created", new Date());

        //using upsert because if no documents found then the doc was removed (SHOULD ONLY HAPPEN BY SOMEONE MANUALLY) so we can just send
        collection.update(new BasicDBObject("_id", id), newMessage, true, false);
    }

    /**
     * Requeue message with earliestGet as Now and 0.0 priority. Same as ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     */
    public void requeue(final BasicDBObject message) {
        requeue(message, new Date());
    }

    /**
     * Requeue message with 0.0 priority. Same as ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     */
    public void requeue(final BasicDBObject message, final Date earliestGet) {
        requeue(message, earliestGet, 0.0);
    }

    /**
     * Requeue message. Same as ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     * @param priority priority for order out of get(). 0 is higher priority than 1. Should not be NaN
     */
    public void requeue(final BasicDBObject message, final Date earliestGet, final double priority) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }

        final Object id = message.get("id");
        if (id.getClass() != ObjectId.class) {
            throw new IllegalArgumentException("id must be an ObjectId");
        }

        final BasicDBObject forRequeue = new BasicDBObject(message);
        forRequeue.removeField("id");
        ackSend(message, forRequeue, earliestGet, priority);
    }

    /**
     * Send message to queue with earliestGet as Now and 0.0 priority
     *
     * @param payload payload. Should not be null
     */
    public void send(final BasicDBObject payload) {
        send(payload, new Date());
    }

    /**
     * Send message to queue with 0.0 priority
     *
     * @param payload payload. Should not be null
     * @param earliestGet earliest instant that a call to Get() can return message. Should not be null
     */
    public void send(final BasicDBObject payload, final Date earliestGet) {
        send(payload, earliestGet, 0.0);
    }

    /**
     * Send message to queue
     *
     * @param payload payload. Should not be null
     * @param earliestGet earliest instant that a call to Get() can return message. Should not be null
     * @param priority priority for order out of Get(). 0 is higher priority than 1. Should not be NaN
     */
    public void send(final BasicDBObject payload, final Date earliestGet, final double priority) {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }

        final BasicDBObject message = new BasicDBObject("payload", payload)
                .append("running", false)
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", earliestGet)
                .append("priority", priority)
                .append("created", new Date());

        collection.insert(message);
    }

    private void ensureIndex(final BasicDBObject index) {
        for (int i = 0; i < 5; ++i) {
            for (String name = UUID.randomUUID().toString(); name.length() > 0; name = name.substring(0, name.length() - 1)) {
                //creating an index with the same name and different spec does nothing.
                //creating an index with different name and same spec does nothing.
                //so we use any generated name, and then find the right spec after we have called, and just go with that name.

                try {
                    collection.ensureIndex(index, new BasicDBObject("name", name).append("background", true));
                } catch (final CommandFailureException e) {
                    //happens when name is too long
                }

                for (final DBObject existingIndex : collection.getIndexInfo()) {

                    if (existingIndex.get("key").equals(index)) {
                        return;
                    }
                }
            }
        }

        throw new RuntimeException("couldnt create index after 5 attempts");
    }
}
