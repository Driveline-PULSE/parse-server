export class FilesAdapter {
  createFile(filename: string, data: any, contentType: string, options?: object): Promise<void>;
  deleteFile(filename: string): Promise<void>;
  getFileData(filename: string): Promise<any>;
  getFileLocation(config: any, filename: string): string | Promise<string>;
}

export function validateFilename(filename: string): { code: number; message: string } | null;

export default FilesAdapter;
