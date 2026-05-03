/**
 * Re-exports the IAdapter interface for cleaner consumer imports.
 *
 * Adapter implementations import from here rather than directly from types.ts.
 */
export type { IAdapter, LogbunLog, LogbunQueryFilters, LogbunQueryResult } from '../types';
