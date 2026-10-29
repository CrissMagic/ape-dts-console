import { Injectable, type OnModuleInit } from '@nestjs/common';
import { HttpAdapterHost } from '@nestjs/core';
import type { FastifyInstance } from 'fastify';

@Injectable()
export class ArtifactUploadParser implements OnModuleInit {
  constructor(private readonly adapterHost: HttpAdapterHost) {}

  onModuleInit() {
    const server = this.adapterHost.httpAdapter.getInstance<FastifyInstance>();
    // No parseAs: hand the untouched stream to the guarded controller. Never buffer uploads.
    server.addContentTypeParser('application/octet-stream', (_request, stream, done) => {
      done(null, stream);
    });
  }
}
