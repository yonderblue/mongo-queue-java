package gaillard.mongo;

import java.util.Calendar;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;

public final class Queue {

    private final MongoCollection<Document> collection;

    public Queue(final MongoCollection<Document> collection) {
        Objects.requireNonNull(collection);

        this.collection = collection;
    }

    /**
     * Ensure index for get() method with no fields before or after sort fields
     */
    public void ensureGetIndex() {
        ensureGetIndex(new Document());
    }

    /**
     * Ensure index for get() method with no fields after sort fields
     *
     * @param beforeSort fields in get() call that should be before the sort fields in the index. Should not be null
     */
    public void ensureGetIndex(final Document beforeSort) {
        ensureGetIndex(beforeSort, new Document());
    }

    /**
     * Ensure index for get() method
     *
     * @param beforeSort fields in get() call that should be before the sort fields in the index. Should not be null
     * @param afterSort fields in get() call that should be after the sort fields in the index. Should not be null
     */
    public void ensureGetIndex(final Document beforeSort, final Document afterSort) {
        Objects.requireNonNull(beforeSort);
        Objects.requireNonNull(afterSort);

        //using general rule: equality, sort, range or more equality tests in that order for index
        final Document completeIndex = new Document("running", 1);

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
        ensureIndex(new Document("running", 1).append("resetTimestamp", 1));//for the stuck messages query in Get()
    }

    /**
     * Ensure index for count() method
     *
     * @param index fields in count() call. Should not be null
     * @param includeRunning whether running was given to count() or not
     */
    public void ensureCountIndex(final Document index, final boolean includeRunning) {
        Objects.requireNonNull(index);

        final Document completeIndex = new Document();

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
    public Document get(final Document query, final int resetDuration) {
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
    public Document get(final Document query, final int resetDuration, final int waitDuration) {
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
    public Document get(final Document query, final int resetDuration, final int waitDuration, long pollDuration) {
        Objects.requireNonNull(query);

        //reset stuck messages
//        collection.update(new Document("running", true).append("resetTimestamp", new Document("$lte", new Date())),
//                new Document("$set", new Document("running", false)),
//                false,
//                true);
        
		collection.updateMany(Filters.and(Filters.eq("running", true),Filters.lte("resetTimestamp", new Date())), 
				Updates.set("running", false), 
				new UpdateOptions().upsert(false));
        

        final Document builtQuery = new Document("running", false);
        for (final Entry<String, Object> field : query.entrySet()) {
            builtQuery.append("payload." + field.getKey(), field.getValue());
        }

        builtQuery.append("earliestGet", new Document("$lte", new Date()));

        final Calendar calendar = Calendar.getInstance();

        calendar.add(Calendar.SECOND, resetDuration);
        final Date resetTimestamp = calendar.getTime();

//        final Document sort = new Document("priority", 1).append("created", 1);
        final Document update = new Document("$set", new Document("running", true).append("resetTimestamp", resetTimestamp));
//        final Document fields = new Document("payload", 1);

        calendar.setTimeInMillis(System.currentTimeMillis());
        calendar.add(Calendar.MILLISECOND, waitDuration);
        final Date end = calendar.getTime();

        while (true) {
//            final Document message = (Document) collection.findAndModify(builtQuery, fields, sort, false, update, true, false);
            final Document message = collection.findOneAndUpdate(builtQuery, update, new FindOneAndUpdateOptions().sort(Sorts.ascending("priority","created")).returnDocument(ReturnDocument.AFTER));
            if (message != null) {
                final ObjectId id = message.getObjectId("_id");
                Document d = ((Document)message.get("payload")).append("id", id);
                Document Document = new Document();
                Document.putAll(d);
                return Document;
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
    public long count(final Document query) {
        Objects.requireNonNull(query);

        final Document completeQuery = new Document();

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
    public long count(final Document query, final boolean running) {
        Objects.requireNonNull(query);

        final Document completeQuery = new Document("running", running);

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
    public void ack(final Document message) {
        Objects.requireNonNull(message);
        final Object id = message.get("id");
        if (id.getClass() != ObjectId.class) {
            throw new IllegalArgumentException("id must be an ObjectId");
        }

        collection.deleteOne(new Document("_id", id));
    }

    /**
     * Ack message and send payload to queue, atomically, with earliestGet as Now and 0.0 priority
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     */
    public void ackSend(final Document message, final Document payload) {
        ackSend(message, payload, new Date());
    }

    /**
     * Ack message and send payload to queue, atomically, with 0.0 priority
     *
     * @param message message to ack received from get(). Should not be null
     * @param payload payload to send. Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     */
    public void ackSend(final Document message, final Document payload, final Date earliestGet) {
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
    public void ackSend(final Document message, final Document payload, final Date earliestGet, final double priority) {
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

        final Document newMessage = new Document("payload", payload)
                .append("running", false)
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", earliestGet)
                .append("priority", priority)
                .append("created", new Date());

        //using upsert because if no documents found then the doc was removed (SHOULD ONLY HAPPEN BY SOMEONE MANUALLY) so we can just send
//        collection.update(new Document("_id", id), newMessage, true, false);
        collection.replaceOne(new Document("_id", id), newMessage, new UpdateOptions().upsert(true));
    }

    /**
     * Requeue message with earliestGet as Now and 0.0 priority. Same as ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     */
    public void requeue(final Document message) {
        requeue(message, new Date());
    }

    /**
     * Requeue message with 0.0 priority. Same as ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     */
    public void requeue(final Document message, final Date earliestGet) {
        requeue(message, earliestGet, 0.0);
    }

    /**
     * Requeue message. Same as ackSend() with the same message.
     *
     * @param message message to requeue received from get(). Should not be null
     * @param earliestGet earliest instant that a call to get() can return message. Should not be null
     * @param priority priority for order out of get(). 0 is higher priority than 1. Should not be NaN
     */
    public void requeue(final Document message, final Date earliestGet, final double priority) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }

        final Object id = message.get("id");
        if (id.getClass() != ObjectId.class) {
            throw new IllegalArgumentException("id must be an ObjectId");
        }

        final Document forRequeue = new Document(message);
        forRequeue.remove("id");
        ackSend(message, forRequeue, earliestGet, priority);
    }

    /**
     * Send message to queue with earliestGet as Now and 0.0 priority
     *
     * @param payload payload. Should not be null
     */
    public void send(final Document payload) {
        send(payload, new Date());
    }

    /**
     * Send message to queue with 0.0 priority
     *
     * @param payload payload. Should not be null
     * @param earliestGet earliest instant that a call to Get() can return message. Should not be null
     */
    public void send(final Document payload, final Date earliestGet) {
        send(payload, earliestGet, 0.0);
    }

    /**
     * Send message to queue
     *
     * @param payload payload. Should not be null
     * @param earliestGet earliest instant that a call to Get() can return message. Should not be null
     * @param priority priority for order out of Get(). 0 is higher priority than 1. Should not be NaN
     */
    public void send(final Document payload, final Date earliestGet, final double priority) {
        Objects.requireNonNull(payload);
        Objects.requireNonNull(earliestGet);
        if (Double.isNaN(priority)) {
            throw new IllegalArgumentException("priority was NaN");
        }

        final Document message = new Document("payload", payload)
                .append("running", false)
                .append("resetTimestamp", new Date(Long.MAX_VALUE))
                .append("earliestGet", earliestGet)
                .append("priority", priority)
                .append("created", new Date());

        collection.insertOne(message);
    }

    private void ensureIndex(final Document index) {
        for (int i = 0; i < 5; ++i) {
            for (String name = UUID.randomUUID().toString(); name.length() > 0; name = name.substring(0, name.length() - 1)) {
                //creating an index with the same name and different spec does nothing.
                //creating an index with different name and same spec does nothing.
                //so we use any generated name, and then find the right spec after we have called, and just go with that name.

//                    collection.ensureIndex(index, new Document("name", name).append("background", true));
                collection.createIndex(index, new IndexOptions().name(name).background(true));

                for (final Document existingIndex : collection.listIndexes()) {

                    if (existingIndex.get("key").equals(index)) {
                        return;
                    }
                }
            }
        }

        throw new RuntimeException("couldnt create index after 5 attempts");
    }
}
