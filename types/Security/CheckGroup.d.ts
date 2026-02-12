export class CheckGroup {
  constructor();
  setName(): string;
  name(): string;
  setChecks(): any[];
  checks(): any[];
  run(): Promise<void>;
}
export default CheckGroup;
