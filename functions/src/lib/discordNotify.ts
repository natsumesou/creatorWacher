import axios from "axios";

/**
 * Discord通知用のクラス
 */
export class Bot {
  private targetWebhook: string;
  private systemWebhook: string;
  private activityWebhook: string;

  /**
   *
   * @param {string} targetWebhook 投稿先チャットの webhook url
   * @param {string} systemWebhook systemチャットの webhook url
   * @param {string} activityWebhook activityチャットの webhook url
   */
  constructor(targetWebhook: string, systemWebhook: string, activityWebhook: string) {
    this.targetWebhook = targetWebhook;
    this.systemWebhook = systemWebhook;
    this.activityWebhook = activityWebhook;
  }

  /**
   *
   * @param {string} message チャットに送信するメッセージ
   */
  async message(message: string) {
    await axios.post(this.targetWebhook, {content: message});
  }

  /**
   *
   * @param {string} message アラートを飛ばすメッセージ
   */
  async alert(message: string) {
    await axios.post(this.systemWebhook, {content: message});
  }

  /**
   *
   * @param {string} message アクティビティを飛ばすメッセージ
   */
  async activity(message: string) {
    await axios.post(this.activityWebhook, {content: message});
  }
}
