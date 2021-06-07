# YouTube Live分析ツール

## 機能概要

登録しておいたチャンネルのYouTube Liveを定期監視し、アーカイブが作成されたらそのアーカイブからチャットログを取得しスパチャやメンバー登録を集計してDiscordに流す。

## 実装概要

1. Cloud SchedulerでYouTubeチャンネルページを定期的にスクレイピングするCloud Functionsを実行
2. 新しい動画がアップロードされたらPub/Subで該当の動画IDをpublish
3. subscribe先のCloud Functionsでチャットログからスパチャやメンバー登録数を算出してfirestoreに保存&Discordに流す

## How to Develop

```
git clone <this_repo_url>
npm install
```

## How to Deploy

Cloud Buildとインテグレーションしてるのでmasterにpushするとデプロイされる

```
git push remote master
```