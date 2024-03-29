/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package kafka.basics;

import java.util.Arrays;

import kafka.demos.consumer.ConsumerDemo;
import kafka.demos.consumer.ConsumerShutdownDemo;
import kafka.demos.producer.ProducerCallbackDemo;
import kafka.demos.producer.ProducerDemo;
import kafka.demos.producer.ProducerKeysDemo;

public class App {
    public static void main(String[] args) {
        String[] runnerArgs = args.length > 1
                ? Arrays.copyOfRange(args, 1, args.length - 1)
                : new String[] {};

        try {
            switch (args[0]) {
                case "producer":
                    ProducerDemo.run(runnerArgs);
                    break;

                case "producerCallback":
                    ProducerCallbackDemo.run(runnerArgs);
                    break;

                case "producerKeys":
                    ProducerKeysDemo.run(runnerArgs);
                    break;

                case "consumer":
                    ConsumerDemo.run(runnerArgs);
                    break;

                case "consumerShutdown":
                    ConsumerShutdownDemo.run(runnerArgs);
                    break;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Please specify at least one runner");
        }
    }
}
