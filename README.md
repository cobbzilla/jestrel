jestrel
=======

Simple Java client for Kestrel

### Motivation

Working with Kestrel queues should be easy. But after scouring the web, I couldn't find a "native" Kestrel client
for Java, only some memcached clients that had some support for Kestrel-specific stuff.

So, building on top of the xmemcached library on googlecode, I wrote jestrel

### System Requirements

* Java 7
* Maven 3

### Building

    mvn package

### Usage

#### Initialize the client library

    // Setup configuration
    Properties kestrelProperties = new Properties();

    // Max # of connections to kestrel. default is 1
    kestrelProperties.setProperty("kestrelConnectionPoolSize", "10");

    // Comma-separated list of hostname:port of all kestrel servers to use. required.
    kestrelProperties.setProperty("kestrelHosts", "kestrel-1:22133, kestrel-2:22133");

    // How often should the client drop its connection to kestrel and reconnect. default is 5 minutes
    // If you are using multiple kestrel servers, this will ensure that no kestrel server sits idle with no clients
    kestrelProperties.setProperty("kestrelReconnectIntervalInMinutes", "5");

    // Create a client
    MqClientFactory clientFactory = new MqClientFactory();
    MqClient client = clientFactory.createClient(KestrelClient.class, kestrelProperties);

### Sending a message

    String queueName = "some_queue";
    MqProducer producer = client.getProducer(queueName);
    producer.send("some message");

### Registering a queue listener

    // I'm using an inline class here for brevity.
    // You will probably want to create your own class that implements the MqConsumer interface.
    MqConsumer consumer = new MqConsumer() {
        /**
         * @param o The message, which will be a String
         * @exception Exception If any exception is thrown, the message will be put onto the errorQueue (unless it was set to null when the listener was registered)
         */
        public void onMessage(Object o) throws Exception {
            System.out.println("received: " + o.toString());
        }
    };

    String queueName = "some_queue";
    String errorQueueName = "some_queue_error";

    client.registerConsumer(consumer, queueName, errorQueueName);
