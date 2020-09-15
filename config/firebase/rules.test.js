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
    'payload': {
      'created': firebase.firestore.FieldValue.serverTimestamp(),
      'uuid': 'preexisting'
      }
    });
});

afterAll(async () => {
  await Promise.all(firebase.apps().map((app) => app.delete()));
});

function getPath(date) {
  return date.toISOString().split('T')[0] + "-"
       + date.toISOString().split('T')[1].split(':')[0];
}

describe('Tests of document writes and access', () => {
  const app = firebase.initializeTestApp({
    projectId: projectId,
    auth: null
  });
  const db = app.firestore()
  const datefmt = getPath(new Date());
  it('document cannot be written at wrong path',
      async () => {
        const doc = db.collection('random').doc('doc');
        await firebase.assertFails(doc.set({}));
      });
  it('document cannot be written without uuid',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc(datefmt)
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.set({}));
      });
  it('document cannot be written without created field',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc(datefmt)
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.set({
          'uuid': 'foo'}));
      });
  it('documents cannot be created at very old path',
      async () => {
        var oldDate = new Date();
        oldDate.setHours(oldDate.getHours() - 2);
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc(getPath(oldDate))
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.set({
        'payload': {
          'created': firebase.firestore.FieldValue.serverTimestamp(),
          'uuid': 'foo'
          }
        }));
      });
  it('correct documents can be created',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc(datefmt)
                      .collection('metrics').doc('testMetric');
        await firebase.assertSucceeds(doc.set({
        'payload': {
          'created': firebase.firestore.FieldValue.serverTimestamp(),
          'uuid': 'foo'
        }
        }));
      });
  it('documents can be created at slightly off path',
      async () => {
        var oldDate = new Date();
        oldDate.setHours(oldDate.getHours() - 1);
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc(getPath(oldDate))
                      .collection('metrics').doc('testMetric');
        await firebase.assertSucceeds(doc.set({
        'payload': {
          'created': firebase.firestore.FieldValue.serverTimestamp(),
          'uuid': 'foo'
          }
        }));
      });
  it('document cannot be deleted',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc(datefmt)
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.delete());
      });
  it('document cannot be updated',
      async () => {
        const doc = db.collection('uuid').doc('foo')
                      .collection('date').doc(datefmt)
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.update({
        'payload': {
          'uuid': 'foo'
          }
        }));
      });
  it('document cannot be read',
      async () => {
        const doc = db.collection('uuid').doc('preexisting')
                      .collection('date').doc('2020-09-03-13')
                      .collection('metrics').doc('testMetric');
        await firebase.assertFails(doc.get());
      });
});