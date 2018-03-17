import * as nats from 'nats';
import { Server } from './server';
import { NO_PATTERN_MESSAGE } from '../constants';
import { MicroserviceOptions } from '../interfaces/microservice-configuration.interface';
import { CustomTransportStrategy, PacketId } from './../interfaces';
import { Observable } from 'rxjs/Observable';
import { catchError } from 'rxjs/operators';
import { empty } from 'rxjs/observable/empty';
import { finalize } from 'rxjs/operators';
import { NATS_DEFAULT_URL, CONNECT_EVENT, ERROR_EVENT } from './../constants';
import { ReadPacket } from './../interfaces/packet.interface';

export class ServerNats extends Server implements CustomTransportStrategy {
  private readonly url: string;
  private consumer: nats.Client;
  private publisher: nats.Client;

  constructor(private readonly options: MicroserviceOptions) {
    super();
    this.url = options.url || NATS_DEFAULT_URL;
  }

  public listen(callback: () => void) {
    this.consumer = this.createNatsClient();
    this.publisher = this.createNatsClient();

    this.handleError(this.publisher);
    this.handleError(this.consumer);
    this.start(callback);
  }

  public start(callback?: () => void) {
    this.bindEvents(this.consumer, this.publisher);
    this.consumer.on(CONNECT_EVENT, callback);
  }

  public bindEvents(consumer: nats.Client, publisher: nats.Client) {
    const registeredPatterns = Object.keys(this.messageHandlers);
    registeredPatterns.forEach(pattern => {
      const channel = this.getAckQueueName(pattern);
      consumer.subscribe(
        channel,
        () => this.getMessageHandler(channel, publisher),
      );
    });
  }

  public close() {
    this.publisher && this.publisher.close();
    this.consumer && this.consumer.close();
    this.publisher = this.consumer = null;
  }

  public createNatsClient(): nats.Client {
    return nats.connect({
      url: this.url,
      json: true,
      maxReconnectAttempts: this.options.retryAttempts,
      reconnectTimeWait: this.options.retryDelay,
    });
  }

  public getMessageHandler(channel: string, pubClient: nats.Client) {
    return async buffer => await this.handleMessage(channel, buffer, pubClient);
  }

  public async handleMessage(
    channel: string,
    message: ReadPacket & PacketId,
    pub: nats.Client,
  ) {
    const pattern = channel.replace(/_ack$/, '');
    const publish = this.getPublisher(pub, pattern, message.id);
    const status = 'error';

    if (!this.messageHandlers[pattern]) {
      return publish({ id: message.id, status, err: NO_PATTERN_MESSAGE });
    }
    const handler = this.messageHandlers[pattern];
    const response$ = this.transformToObservable(
      await handler(message.data),
    ) as Observable<any>;
    response$ && this.send(response$, publish);
  }

  public getPublisher(publisher: nats.Client, pattern: any, id: string) {
    return response =>
      publisher.publish(
        this.getResQueueName(pattern, id),
        Object.assign(response, { id }) as any,
      );
  }

  public getAckQueueName(pattern: string): string {
    return `${pattern}_ack`;
  }

  public getResQueueName(pattern: string, id: string): string {
    return `${pattern}_${id}_res`;
  }

  public handleError(stream) {
    stream.on(ERROR_EVENT, err => this.logger.error(err));
  }
}
