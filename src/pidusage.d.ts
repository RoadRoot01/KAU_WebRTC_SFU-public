declare module 'pidusage' {
  type ProcessStats = {
    cpu: number;
    memory: number;
  };

  function pidusage(pids: number[]): Promise<Record<string, ProcessStats>>;

  export default pidusage;
}
