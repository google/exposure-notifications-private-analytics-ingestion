# Firebase Config

Install the [Firebase CLI](https://firebase.google.com/docs/cli) and login as
follows:

```shell script
firebase login
```

## Firestore Security Rules

This provides a sample way to configure Firestore documents for
Exposure Notifications Private Analytics.

### Testing

First install the emulator, npm testing library and jest:

```shell script
firebase setup:emulators:firestore
npm init -y
npm i @firebase/testing
npm i jest
```

Then start the emulator and execute the test script:

```shell script
firebase emulators:exec --only firestore "npm run test"
```

### Deploying

You can update your projects Firestore Security Policy with these rules as
follows:

```shell script
firebase deploy --only firestore:rules
```

## Remote Config

`remote-config.json` contains the dynamic configuration used by the ENPA module
in the EN Express app.

### Deploying

To update the remote config, edit the file and then get a bearer token for the
service account you wish to authenticate with:

```shell script
gcloud auth application-default print-access-token
```

This will use the key in the json pointed to by the `GOOGLE_APPLICATION_CREDENTIALS`
environment variable.

Then you can use the REST API to update the config. Note that this does a forced
update without respecting the ETAG of the current version. Issue a Get request
prior to this command and use that ETAG to be safe.

```shell script
curl --compressed -H "Content-Type: application/json; UTF8" -H "If-Match: *" -H "Authorization: Bearer ${TOKEN}" -X PUT https://firebaseremoteconfig.googleapis.com/v1/projects/${PROJECT}/remoteConfig -d @remote-config.json
```