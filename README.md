# fsharp-amqplib
F# client for AMQP

AMQP is an efficient messaging protocol - https://www.rabbitmq.com/

This AMQP client was originally created while learning F#.  My goal was to mimic the C# library's functionality.
The C# code is about 25K LOC.  Much to my surprise, I was able to match the functionality of C# in just 2,500 LOC!
I was also able to easily include additional AMQP features.

The AMQP protocol supports two modes of operation.  In one mode, performance is a key requirement and a dropped or lost message is OK.
In the second mode, message delivery must be guaranteed.  The client must receive a server Ack, otherwise it must resend the message.
A secondary goal was to efficiently support both these client modes.

I believe F# is superior to C#.  You be the judge!

Thanks for your support.
