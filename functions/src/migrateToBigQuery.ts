import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {ChangeType, migrateStreamsToBigQuery} from "./exportToBigQuery";
import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";

export const migrateToBigQuery = async () => {
  const db = admin.firestore();
  const channels = await db.collection("channels").where("category", "==", "hololive").get().catch((err) => {
    functions.logger.error(err.message + "\n" + err.stack);
  });

  if (!channels || channels && channels.empty) {
    return;
  }

  const tempChannels: FirebaseFirestore.QueryDocumentSnapshot<FirebaseFirestore.DocumentData>[] = [];

  channels.forEach((channel) => {
    tempChannels.push(channel);
  });

  for (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!streams || streams && streams.empty) {
      continue;
    }

    const tempStreams: DocumentSnapshot[] = [];
    let count = 0;
    streams.forEach((stream) => {
      if (count < 3) {
        tempStreams.push(stream);
        count += 1;
      }
    });

    functions.logger.info("migrate channel videos: " + tempStreams.length);
    await migrateStreamsToBigQuery(channel, tempStreams, ChangeType.CREATE);
    break;
  }
};
