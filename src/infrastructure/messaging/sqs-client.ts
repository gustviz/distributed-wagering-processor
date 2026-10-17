import { GetQueueUrlCommand, SQSClient } from '@aws-sdk/client-sqs';
import type { AppConfig } from '../../config/app-config';

export function createSqsClient(config: AppConfig): SQSClient {
  return new SQSClient({
    region: config.aws.region,
    endpoint: config.aws.endpoint,
    credentials: { accessKeyId: config.aws.accessKeyId, secretAccessKey: config.aws.secretAccessKey },
    // With an emulator endpoint, never route to the host inside QueueUrl (it may be
    // "localhost.localstack.cloud", unreachable from containers).
    useQueueUrlAsEndpoint: config.aws.endpoint === undefined,
    maxAttempts: 3,
    requestHandler: { requestTimeout: 30_000, connectionTimeout: 3_000 },
  });
}

/** Resolves and caches queue URLs (queues are provisioned by infra, not by the app). */
export class QueueUrls {
  private readonly cache = new Map<string, Promise<string>>();

  constructor(private readonly client: SQSClient) {}

  get(queueName: string): Promise<string> {
    let url = this.cache.get(queueName);
    if (!url) {
      url = this.client.send(new GetQueueUrlCommand({ QueueName: queueName })).then((r) => {
        if (!r.QueueUrl) throw new Error(`Queue ${queueName} not found`);
        return r.QueueUrl;
      });
      url.catch(() => this.cache.delete(queueName));
      this.cache.set(queueName, url);
    }
    return url;
  }
}
