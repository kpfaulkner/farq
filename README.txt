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

