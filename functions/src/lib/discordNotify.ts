import {Client, TextChannel} from "discord.js";

const GENERAL_CHANNEL_ID = "848982018577793068";
const SYSTEM_CHANNEL_ID = "849651263410667520";

/**
 * Discord通知用のクラス
 */
export class Bot {
  private client: Client;

  /**
   *
   * @param {string} token Discord botのTOKEN
   */
  constructor(token: string) {
    this.client = new Client();
    this.client.login(token);
  }

  /**
   *
   * @param {string} message チャットに送信するメッセージ
   */
  message(message: string) {
    (this.client.channels.cache.get(GENERAL_CHANNEL_ID) as TextChannel).send(message);
  }

  /**
   *
   * @param {string} message アラートを飛ばすメッセージ
   */
  alert(message: string) {
    (this.client.channels.cache.get(SYSTEM_CHANNEL_ID) as TextChannel).send(message);
  }
}
