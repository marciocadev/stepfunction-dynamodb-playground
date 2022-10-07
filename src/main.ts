import { App, RemovalPolicy, Stack, StackProps } from 'aws-cdk-lib';
import { AttributeType, Table } from 'aws-cdk-lib/aws-dynamodb';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Chain, JsonPath, Parallel, Pass, StateMachine, Map } from 'aws-cdk-lib/aws-stepfunctions';
import { CallAwsService, DynamoAttributeValue, DynamoPutItem, LambdaInvoke } from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';
import { join } from 'path';

export class MyStack extends Stack {
  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const table = new Table(this, 'Dynamo', {
      tableName: 'step-functions-dynamo',
      partitionKey: { name: 'pk', type: AttributeType.STRING },
      sortKey: { name: 'sk', type: AttributeType.STRING },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const pass = new Pass(this, 'Pass', {
      parameters: {
        'pk.$': '$.pk',
        'sk.$': '$.sk',
        'str.$': '$.str',
        'num.$': '$.num',
        'map.$': '$.map',
        'strLst.$': '$.strLst',
        'numLst.$': '$.numLst',
        'mapLst.$': '$.mapLst',
        'bool.$': '$.bool',
        'convertToBinary.$': '$.convertToBinary',
        'batch.$': '$.batch',
      },
    });

    const batchPass = new Pass(this, 'BatchPass', {
      parameters: {
        'batch.$': '$.batch',
      }
    });
    const lmbBatch = new NodejsFunction(this, 'BatchLambda', {
      entry: join(__dirname, 'lambda-fns/batch.ts'),
      handler: 'handler'
    });
    const lmbBatchTask = new LambdaInvoke(this, 'BatchLambdaInvoke', {
      lambdaFunction: lmbBatch,
      outputPath: '$.Payload',
    })
    const tableBatchTask = new CallAwsService(this, 'BatchWriteItem', {
      service: 'dynamodb',
      action: 'batchWriteItem',
      iamResources: [table.tableArn],
      parameters: {
        RequestItems: {
          'step-functions-dynamo': JsonPath.listAt('$.data')
        }
      }
    })
    batchPass.next(lmbBatchTask).next(tableBatchTask);

    const singlePass = new Pass(this, 'SinglePass', {
      parameters: {
        'single': {
          'pk.$': '$.pk',
          'sk.$': '$.sk',
          'str.$': '$.str',
          'num.$': '$.num',
          'map.$': '$.map',
          'strLst.$': '$.strLst',
          'numLst.$': '$.numLst',
          'mapLst.$': '$.mapLst',
          'bool.$': '$.bool',
          'binary.$': '$.convertToBinary',
        }
      },
    });

    const mapNumLst = new Map(this, 'MapNumList', {
      inputPath: '$.numLst',
      maxConcurrency: 0,
      resultPath: '$.numLstProcess',
    });
    const passNumLst = new Pass(this, 'NumListPass', {
      parameters: {
        num: DynamoAttributeValue.numberFromString(JsonPath.stringAt('States.Format(\'{}\', $)')),
      },
      outputPath: '$.num.attributeValue'
    });
    mapNumLst.iterator(passNumLst);

    const mapMapLst = new Map(this, 'MapMapList', {
      inputPath: '$.mapLst',
      maxConcurrency: 0,
      resultPath: '$.mapLstProcess',
    });
    const passMapLst = new Pass(this, 'MapListPass', {
      parameters: {
        map: DynamoAttributeValue.fromMap({
          str: DynamoAttributeValue.fromString(JsonPath.stringAt('$.str')),
          num: DynamoAttributeValue.numberFromString(JsonPath.stringAt('States.Format(\'{}\', $.num)')),
        }),
      },
      outputPath: '$.map.attributeValue',
    });
    mapMapLst.iterator(passMapLst);

    const putItem = new DynamoPutItem(this, 'PutItem', {
      item: {
        pk: DynamoAttributeValue.fromString(JsonPath.stringAt('$[0].single.pk')),
        sk: DynamoAttributeValue.fromString(JsonPath.stringAt('$[0].single.sk')),
        str: DynamoAttributeValue.fromString(JsonPath.stringAt('$[0].single.str')),
        num: DynamoAttributeValue.numberFromString(JsonPath.stringAt('States.JsonToString($[0].single.num)')),
        map: DynamoAttributeValue.fromMap({
          'strMap': DynamoAttributeValue.fromString(JsonPath.stringAt('$[0].single.map.strMap')),
          'numMap': DynamoAttributeValue.numberFromString(JsonPath.stringAt('States.JsonToString($[0].single.map.numMap)')),
        }),
        strLst: DynamoAttributeValue.listFromJsonPath(JsonPath.stringAt('$[0].single.strLst')),
        // strSet: DynamoAttributeValue.fromStringSet(JsonPath.listAt('$[0].single.strLst')),
        bool: DynamoAttributeValue.booleanFromJsonPath(JsonPath.stringAt('$[0].single.bool')),
        binary: DynamoAttributeValue.fromBinary(JsonPath.stringAt('$[0].single.binary')),
        numLst: DynamoAttributeValue.listFromJsonPath(JsonPath.stringAt('$[1].numLstProcess')),
        mapLst: DynamoAttributeValue.listFromJsonPath(JsonPath.stringAt('$[2].mapLstProcess'))
      },
      table: table,
      resultPath: JsonPath.DISCARD,
    });

    const parallelTwo = new Parallel(this, 'ParallelTwo', {
      resultPath: '$',
    });
    parallelTwo.branch(singlePass).branch(mapNumLst).branch(mapMapLst);
    parallelTwo.next(putItem);

    const parallelOne = new Parallel(this, 'ParallelOne', {
      resultPath: JsonPath.DISCARD,
    });
    parallelOne.branch(batchPass).branch(parallelTwo);

    const chain = Chain.start(pass)
      .next(parallelOne);
    new StateMachine(this, 'StateMachine', {
      stateMachineName: 'step-functions-dynamo',
      definition: chain,
    });
  }
}

// for development, use account/region from cdk cli
const devEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const app = new App();

new MyStack(app, 'stepfunctions-dynamodb-playground-dev', { env: devEnv });
// new MyStack(app, 'stepfunctions-dynamodb-playground-prod', { env: prodEnv });

app.synth();