#!/usr/bin/env node
// Run on a trusted deployment host with Docker, never inside the platform container.
import process from 'node:process';
import {
  runCertificationCli,
  reportCertificationFailure,
} from '../apps/api/dist/artifacts/certification-cli.js';
await runCertificationCli(process.argv.slice(2)).catch(reportCertificationFailure);
