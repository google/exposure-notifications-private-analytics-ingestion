## Run Firebase integration tests
echo "************ Running Firebase integration tests script ************"
## Get the directory of the build script
scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))
## cd to the parent directory, i.e. the root of the git repo
cd ${scriptDir}/..

cd config/firebase

echo "************ Installing Firebase emulator, npm testing library and jest ************"
apt update
apt install -y npm
npm install -g firebase-tools
firebase setup:emulators:firestore
npm init -y
npm i @firebase/testing
npm i jest
echo "************ Dependencies installed successfully! ************"

echo "************ Executing rules.test.js ************"
firebase emulators:exec --only firestore "npm run test"
