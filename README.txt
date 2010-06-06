Fast Asynchronous Reliable Queue (FARQ):
========================================

FARQ is a simple queue system written in Scala. So far it's purely used to "scratch my own itch" but am also using it as a way of 
forcing myself to learn more and more Scala.

This is a branch for making the queue elements persist to storage immediately as opposed to waiting until an in memory buffer gets full.
Unsure how this will go performance wise, but want to test.


Building:
---------

Scala 2.7.* and maven 2.*.* are required.

mvn clean install


Running:
--------

scala -cp farq-0.1-jar-with-dependencies.jar FARQ.farq

The queue will then be listening on the port defined in farq.cfg 

Configuration:
--------------

Edit farq.cfg to your own specification.

port = Port number server uses.
max_handlers = Maximum number of concurrent users.
queue_timeout_ms = Socket timeouts.
queue_dir = Directory where the persistent queue will be stored.
queue_size = Queue size allowed in memory, if it goes over it will be serialized to disk.
backlog = Socket accept backlog.
server_ip = server ip.


Design Notes:
-------------

(just thoughts/ramblings)
Serialization to disk:

FARQueue has 2 queues. One that is purely memory, used for when the consumers are outpacing the producers.
The second memory queue is populated purely from deserialized queues from disk.

The only tricky part is when we have many persisted queues and the consumers are outpacing the producers and we have
to switch from read behind mode to purely from memory. The plan is that once we're down to a single persisted file
(which means it's opened for writing ) we flick back the switch to tell the reader to retrieve from the "setting" memory
queue. This simplistic method will only work if when we persist the memory Q we also clear it (and not try and keep it
up to populated). This way when we restore the second last persisted file (the last being the one currently being written
too) we *know* that the in memory and last file are the same. Then we just "flick the switch" to say read off memory Q again.
FARK...

THAT WAS EXPLAINED BADLY, REWORD IT!!!!

