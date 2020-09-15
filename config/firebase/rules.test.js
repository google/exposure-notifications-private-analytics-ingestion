const firebase = require('@firebase/testing');
const fs = require('fs');
const path = require('path');

const projectId = "emulator-test-project"

beforeAll(async () => {
  await firebase.loadFirestoreRules({
    projectId: projectId,
    rules: fs.readFileSync("firestore.rules", "utf8")
  });
  const app = firebase.initializeAdminApp({ projectId: projectId });
  const doc = app.firestore().collection('uuid').doc('preexisting')
                .collection('date').doc('2020-09-03-13')
                .collection('metrics').doc('testMetric');
  await doc.set({
    'created': firebase.firestore.FieldValue.serverTimestamp(),
    'uuid': 'preexisting'});
});

afterAll(async () => {
  await Promise.all(firebase.apps().map((app) => app.delete()));
});

describe('Tests of document writes and access', () => {
  const app = firebase.initializeTestApp({
    projectId: projectId,
    auth: null
  });
  const db = app.firestore()
  it('document cannot be written without uuid',
      async () => {
        const doc = db.collection('uuid').doc('foo');
        await firebase.assertFails(doc.set({}));
      });
  it('document cannot be written without created',
      async () => {
        const doc = db.collection('uuid').doc('foo');
        await firebase.assertFails(doc.set({
          'uuid': 'foo'}));
      });
  it('correct documents can be created',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc('2020-09-03-14')
                      .collection('metrics').doc('testMetric');
        await firebase.assertSucceeds(doc.set({
          'created': firebase.firestore.FieldValue.serverTimestamp(),
          'uuid': 'foo'}));
      });
  it('document cannot be deleted',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc('2020-09-03-14')
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.delete());
      });
  it('document cannot be updated',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc('2020-09-03-14')
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.update({'uuid': 'foo'}));
      });
  it('document cannot be read',
      async () => {
        const doc = db.collection('uuid').doc('preexisting')
                      .collection('date').doc('2020-09-03-13')
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.get());
      });
});