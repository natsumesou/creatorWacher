import * as functions from "firebase-functions";
import * as admin from "firebase-admin";
import {ChangeType, migrateStreamsToBigQuery, migrateSuperChatsToBigQuery} from "./exportToBigQuery";
import {DocumentSnapshot} from "firebase-functions/lib/providers/firestore";

export const migrateToBigQuery = async () => {
  await migrateStreams();
  await migrateSuperChats();
};

const migrateStreams = async () => {
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

  for await (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!streams || streams && streams.empty) {
      continue;
    }

    const tempStreams: DocumentSnapshot[] = [];
    streams.forEach((stream) => {
      tempStreams.push(stream);
    });

    functions.logger.info("migrate channel videos: " + tempStreams.length);
    await migrateStreamsToBigQuery(channel, tempStreams, ChangeType.CREATE);
    break; // check
  }
};

const migrateSuperChats = async () => {
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

  for await (const channel of tempChannels) {
    const streams = await channel.ref.collection("streams").get().catch((err) => {
      functions.logger.error(err.message + "\n" + err.stack);
    });

    if (!streams || streams && streams.empty) {
      continue;
    }

    const tempStreams: DocumentSnapshot[] = [];
    let count = 0;
    streams.forEach((stream) => {
      if (count < 1) {
        tempStreams.push(stream);
        count += 1;
      }
    });

    for await (const stream of tempStreams) {
      const superChats = await stream.ref.collection("superChats").get().catch((err) => {
        functions.logger.error(err.message + "\n" + err.stack);
      });

      if (!superChats || superChats && superChats.empty) {
        continue;
      }

      const tempSuperChats: DocumentSnapshot[] = [];
      superChats.forEach((sc) => {
        tempSuperChats.push(sc);
      });
      functions.logger.info("migrate channel sc: " + tempStreams.length);
      await migrateSuperChatsToBigQuery(tempSuperChats, channel.id, stream.id, ChangeType.CREATE);
      break; // check
    }
    break; // check
  }
};
