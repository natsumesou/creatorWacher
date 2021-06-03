import axios from "axios";

/**
 * Discord通知用のクラス
 */
export class Bot {
  private generalWebhook: string;
  private systemWebhook: string;

  /**
   *
   * @param {string} generalWebhook 一般チャットの webhook url
   * @param {string} systemWebhook systemチャットの webhook url
   */
  constructor(generalWebhook: string, systemWebhook: string) {
    this.generalWebhook = generalWebhook;
    this.systemWebhook = systemWebhook;
  }

  /**
   *
   * @param {string} message チャットに送信するメッセージ
   */
  async message(message: string) {
    await axios.post(this.generalWebhook, {content: message});
  }

  /**
   *
   * @param {string} message アラートを飛ばすメッセージ
   */
  async alert(message: string) {
    await axios.post(this.systemWebhook, {content: message});
  }
}
