CommManager
===========

The CommManager is a generic high-level Java API for communication using ZeroMQ and Protocol Buffers. The combination of these two tools provides for high-performance flexible communication. The API on top of it allows for easy high-level usage on top.

The files relevant to the logic are:
* WiSpearComm.proto - Protocol Buffer definition for messages.
* CommManager.java - Class supporting the High-Level API.
* MessageHandler.java - Helper class.
* Messages.java - Class generated by the Protocol Buffer compiler.
* Test.java - A demo usage of the CommManager. Runs on Eclipse out-of-the-box.

See full documentation in code.
