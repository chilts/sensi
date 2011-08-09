About
=====

sensi-queue.js is a queue infrastructure service. This simple queue provides
the ability to add, get and ack messages but also provides other useful
information such as the time the message was added and the number of
attempted deliveries. It has a default queue but you can use as many queues
as you require which are automatically created.

Operations
----------

All operations are performed with a 'GET' request and all return a JSON data
structure.

The data structure consists of a 'status' field with an optional 'data'
field. The 'status' structure has a boolean 'ok' field, an integer 'code' field
and a string 'msg' field.

    {"status":{"ok":true,"code":0,"msg":"Message Added"}}

If 'ok' is true, 'code' is always 0. If 'ok' is false, 'code' is always a
positive number.

Public Operations
-----------------

There are only three public methods and that's basically all you need. This
keeps things simple from your point of view.

To add a message to a queue:

    $ curl http://localhost:8000/add?msg=Hello
    {"status":{"ok":true,"code":0,"msg":"Message Added"}}

To retrieve a message from the queue:

    $ curl http://localhost:8000/get
    {...,"data":{...,"msg":"Hello","token":"xK3wNKWS",...}

To acknowledge a message has been processes:

    $ curl http://localhost:8000/ack?token=xK3wNKWS
    {"status":{"ok":true,"code":0,"msg":"Message Successfully Acked, ..."}}

Each of these also takes a 'queue' parameter so you know which queue you're
talking to. In the case of not providing a 'queue' parameter, it is taken to be
the 'default' queue. You may specify this explicitly if you like.

Private Operations
------------------

There are some methods that you really shouldn't use since they are used
internal by sensi-queue.js to perform certain functions such as
replication. However, there is nothing stopping you using them if you know
what you are doing and even though they are private they can be called via a
public means. They are however documented here for sake of
completeness. Internal methods are prefixed with the '_' (underscore)
character so you are reminded of what you're doing.

Note: internal methods come with no guarantees that their API will stay the
same. Usually public methods will stay the same or at least will try to be
maintained, however private methods are not held to the same
requirements. You have been warned but also, keep having fun! :)

To delete a message outright:

    $ curl http://localhost:8000/_del?id=7q.g456G
    {"status":{"ok":true,"code":0,"msg":"Message Deleted"}}

This method is private since it is used by sensi-queue.js when dealing with
replicated messages. ie. if an ack is received for a message to one node, it
will attempt to delete it from all the other nodes in the cluster using this
command.

To replicate a message:

    $ curl 'http://localhost:8000/_rep?id=7q.g456G&...'

Note about returned the 'ok' field
----------------------------------

You may note that when you try and get a message from an empty queue, the
status.ok is true when you might have expected it to be false. This is correct
since 'ok' is only false if the operation failed. In this case the operation
suceeded, it just so happened that there were no messages to return. This is an
expected situation.
