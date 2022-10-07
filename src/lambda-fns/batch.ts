import 'source-map-support/register';
import { marshall } from '@aws-sdk/util-dynamodb';

interface payload {
  batch: {[key:string]: any}[]
}

export const handler = async(event: payload) => {

  console.log(event);
  let data = [];
  for (let item of event.batch) {
    data.push({
      PutRequest: {
        Item: marshall(item)
      }
    })
  }
  return {data: data};
}