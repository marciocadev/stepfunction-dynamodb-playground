import { awscdk } from 'projen';
const project = new awscdk.AwsCdkTypeScriptApp({
  cdkVersion: '2.45.0',
  defaultReleaseBranch: 'main',
  name: 'stepfunctions-dynamodb-playground',
  projenrcTs: true,

  deps: [
    '@aws-sdk/util-dynamodb',
    'source-map-support'
  ],
});
project.synth();