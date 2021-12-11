/**
 * 為替レートを管理するクラス
 * TODO 将来的にレートの更新を自動化したい
 */
export class ExchangeRateManager {
  /**
   * @param {string} currencyCode ISO 4217フォーマットの通貨コード
   * @return {number} 日本円のレート
   */
  getCurrentRate(currencyCode: string) {
    switch (currencyCode) {
      case "$":
        return 110.0;
      case "A$":
        return 84.48;
      case "CA$":
        return 91.12;
      case "CHF":
        return 122.08;
      case "COP":
        return 0.030;
      case "HK$":
        return 14.22;
      case "HUF":
        return 0.39;
      case "MX$":
        return 5.47;
      case "NT$":
        return 3.97;
      case "NZ$":
        return 78.80;
      case "PHP":
        return 2.31;
      case "PLN":
        return 29.98;
      case "R$":
        return 21.72;
      case "RUB":
        return 1.51;
      case "SEK":
        return 13.23;
      case "£":
        return 155.61;
      case "₩":
        return 0.099;
      case "€":
        return 133.77;
      case "₹":
        return 1.51;
      case "¥":
        return 1;
      case "PEN":
        return 28.84;
      case "ARS":
        return 1.16;
      case "CLP":
        return 0.15;
      case "NOK":
        return 13.16;
      case "BAM":
        return 68.76;
      case "SGD":
        return 83.05;
      case "CZK":
        return 5.25;
      case "ZAR":
        return 8.09;
      case "RON":
        return 27.18;
      case "BYN":
        return 43.50;
      case "₱":
        return 2.31;
      case "MYR":
        return 26.76;
      case "₪":
        return 33.87;
      case "DKK":
        return 17.99;
      case "CRC":
        return 0.18;
      case "SAR":
        return 29.21;
      case "AED":
        return 29.84;
      case "UYU":
        return 2.51;
      case "HNL":
        return 4.58;
      case "MAD":
        return 12.49;
      case "BGN":
        return 68.16;
      case "GTQ":
        return 14.29;
      case "EGP":
        return 7.03;
      case "ISK":
        return 0.91;
      case "BOB":
        return 15.99;
      case "RSD":
        return 1.14;
      case "TRY":
        return 12.66;
      case "HRK":
        return 17.82;
      case "DOP":
        return 1.92;
      case "PYG":
        return 0.016;
      case "NIO":
        return 3.14;
      case "QAR":
        return 30.43;
      case "KES":
        return 1.01;
      default:
        throw new Error("為替レートの処理中にエラーが発生しました: " + currencyCode);
    }
  }
}
