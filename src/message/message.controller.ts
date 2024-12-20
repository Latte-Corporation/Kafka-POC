import { Controller, Get, Header, Param, Post, Sse } from '@nestjs/common';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { map, Subject } from 'rxjs';
import { Message } from './dto/message';

@Controller('message')
export class MessageController {
  private readonly messagesSubject = new Subject<Message>();
  private readonly kafkaInstance: Kafka;
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  
  constructor(
  ) {
    this.kafkaInstance = new Kafka({
      clientId: "message-service",
      brokers: [process.env.KAFKA_BROKER],
      connectionTimeout: 3000,
      authenticationTimeout: 1000,
      reauthenticationThreshold: 10000,
    });

    this.producer = this.kafkaInstance.producer();
    this.consumer = this.kafkaInstance.consumer({ groupId: 'message-service' });
  }

  async onModuleInit() {
    try {
      await this.producer.connect();
      console.log('Kafka producer connected');

      await this.consumer.connect();
      console.log('Kafka consumer connected');

      this.consumerSubscribe();
    } catch (error) {
      console.error('Error during Kafka setup:', error);
    }
  }

  async consumerSubscribe() {
    await this.consumer.subscribe({ topic: 'message-service', fromBeginning: true });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value?.toString();
        if (value) {
          const parsedMessage: Message = JSON.parse(value);
          console.log(`Received message: ${JSON.stringify(parsedMessage)}`);
          this.messagesSubject.next(parsedMessage);
        }
      },
    });
  }

  @Post(':id')
  async sendMessage(
    @Param('id') id: string,
  ) {
    const message: Message = {
      content: 'Hello from the message service!',
      timestamp: Date.now(),
      serverId: id,
    };

    await this.producer.send({
      topic: 'message-service',
      messages: [
        { value: JSON.stringify(message) },
      ],
    });
  }

  @Sse('server/:id')
  stream(
    @Param('id') id: string,
  ) {
    return this.messagesSubject
      .asObservable()
      .pipe(map((data) => {
        if (data.serverId == id) {
          return { data };
        }
      }));
  }

}
