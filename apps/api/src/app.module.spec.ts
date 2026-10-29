import { Test } from '@nestjs/testing';
import { expect, it } from 'vitest';
import { AppModule } from './app.module.js';
import { AlertsService } from './alerts/alerts.service.js';
import { TaskProgressService } from './task-progress/task-progress.service.js';

it('resolves the complete application graph including alert and progress dependencies', async () => {
  const module = await Test.createTestingModule({ imports: [AppModule] }).compile();
  try {
    expect(module.get(AlertsService)).toBeInstanceOf(AlertsService);
    expect(module.get(TaskProgressService)).toBeInstanceOf(TaskProgressService);
  } finally {
    await module.close();
  }
});
