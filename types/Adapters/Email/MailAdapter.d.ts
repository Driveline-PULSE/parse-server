export class MailAdapter {
  sendMail(options: { to: string; text: string; subject: string; [key: string]: any }): void;
}
export default MailAdapter;
