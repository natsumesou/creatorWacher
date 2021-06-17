import * as crypto from "crypto";
import axios from "axios";

export const upload = async (filename: string, text: string) => {
  const credentials = {
    private_key: "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDRw5KLckPFi43+\nI8e3UKR2nLTYHUq9CQ0BtF5Gw/YX5Q011vU5+VPSlVtGTH4mFmavorHDrksykvSm\nyGSmFk9YsEeL5iW6sI/FiNMY8/rX37d0pAzCDWyH3U+jBOWMzvGvFI64WRjmtxAi\n0wTDs/mbiHeeanhVxdmzPG1uie8UVX9bDX/TfWFwarMtdYKl79Ev167c7Dzvwc5V\nlj7UhRUy42obfPUgui2UWUFL6l+cV8TcHMuNYogj134L9j82saBg1uMCc2TGSAmZ\nqffJNjdRnoj5xzFZ7DnPZAtCOsm2YjbXoEAv6soVePwh5zzDRUcIpDPCrbSnPkBz\nvboDOuTrAgMBAAECggEAIiXxfGwMpmMyqXXPtIMXUwvt0OTr3txTVryzQFf0fy1V\nujcVUuvI/pY79rTLMI4jB2K4B8AfE8uismgbIoKtRiXkb9z2tW666RB+NSi65FvS\nNNXkEc83pOKdbU+FwyeJZtQ068PtbtnMca7irq1k7DIKLyrsDiKYbzpeIkmoNyns\nd5QP7WFW6eSTTC1u2mTsIBglwT01RFCdxWWdU0WYSGIrFsEX1t1zvXe6K+N56XNj\novE4wOnnNbHEWIyeqyvBPEwhQt7D4TBrvsrtmtefpQoy6TU1RFRlrA53IK+6Tsee\nboQICQArkqYWdihTFFD223iyxClpL2j4VCpW4tG6yQKBgQDzj9dCK3nieBTcemUo\ny+jC9m7We1KjdfAcYZTz1kFZI5L9sOBJ+uMBhYQrgD/4HmuFTTtbwhcOwGof3cxf\n/0aPY6AjUpmlnEyEy2of3YXFz6oBUXKYNepGsTbvP2cgnHx1aBdSOizvnTudkXsn\nvaQSFYhzS+P5dV054kIHzWK4hwKBgQDceeGG+UDkOOrdkWG3aU48SzUFK5CQuq4D\nS4dVk9+v60NiDXXCQXm41kodvz6sMMvmuLsZspzKF6cAl9x7z63DE7AsPTodm8Im\nHgQJ+q4zP9RUg9TjAfFw9Z8tEVK2gIZY73CV7COHhjBWbI4f/we1T6SQbgy3XCdu\nuewcPUSdfQKBgQDJySn10D8yuXnPEDgvDIDSxYgeeh2E/3jmipH1UlThnO9y7s9j\ne1AHOq69u4tD7S/3SH0dEDg6KH/D1uYzwjKbKVbK7OwOoOdkPYK207i4ocufO/NM\n15444yk4Hbc69WrHem2X2krQWdvCN31o91yu/tgFbaJ6UhnikulRvX9EkQKBgCPq\nZubJ69xBuWDmsfhi2y/PQI/bDO3Gu1omD6HNOP87X/q1Hp3KdL27is9VvCvotw6a\nwT/qbMgvGjFqi6xPpIrGxkguoSz5lqMG+Ll3cLFAkKJD54YjZuVz3b5FuCeqwDf0\nqW6CzTbikTVC8dQcg3DxzGkKxF+KT5ImD2R6RpcxAoGABF9P1j2Na6ZS1HtAxDMc\nk0Js4JgpblB0+k8kYtvWYotJxF/kB70CRCQX9ZCXgql7Eg+IjCDIIv+kC18COVBd\n2LvvupuafvsbAfmR33tjSsEAVQPwR7vpxi5pyILPU+y/gWjb+XjArkEMA5lIHfCQ\ndopWMEhO91uNLOUc4DdQ/i8=\n-----END PRIVATE KEY-----\n",
    client_email: "storage-manager-from-gas@discord-315419.iam.gserviceaccount.com",
  };
  const method = "PUT";
  const targetURL = "https://storage.googleapis.com/vtuber.ytubelab.com/tmp/" + filename;
  const url = getSignedURL(credentials, targetURL, 1120, method, "application/octet-stream");
  const options = {
    headers: {
      "content-type": "application/octet-stream",
      "Cache-Control": "public, max-age=3600",
    },
  };
  try {
    await axios.put(url, text, options);
  } catch (err) {
    console.error(err.message);
  }
};

const getSignedURL = (credential: any, baseURL: string, TTLInSeconds: number, httpVerb: string, contentType: string) => {
  const unixEpochDateInMilliseconds = Date.now();
  const unixEpochDateInSeconds = Math.floor(unixEpochDateInMilliseconds/1000);
  const expiration = unixEpochDateInSeconds + TTLInSeconds;
  const contentMD5 = "";
  const canonicalizedExtensionHeaders = "";
  const canonicalizedResource = baseURL.split("https://storage.googleapis.com")[1];
  const stringToSign = httpVerb + "\n" +
    contentMD5 + "\n" +
    contentType + "\n" +
    expiration + "\n" +
    canonicalizedExtensionHeaders +
    canonicalizedResource;

  const privateKey = credential.private_key;
  const googleAccessStorageId = credential.client_email;
  const signatureBase64Encoded = crypto.createSign("RSA-SHA256").update(stringToSign).sign(privateKey, "base64");
  const signatureURIEncoded = encodeURI(signatureBase64Encoded);

  const params = {
    GoogleAccessId: googleAccessStorageId,
    Expires: expiration,
    Signature: signatureURIEncoded,
  };

  return buildUrl_(baseURL, params);
};

const buildUrl_ = (url:string, params: any) => {
  const paramString = Object.keys(params).map((key) => {
    return encodeURIComponent(key) + "=" + encodeURIComponent(params[key]);
  }).join("&");
  return url + (url.indexOf("?") >= 0 ? "&" : "?") + paramString;
};
