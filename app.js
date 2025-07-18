import axios from 'axios';
import mysql from "mysql2/promise";
import dotenv from 'dotenv';
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url';
import { dirname } from 'path';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Validate required environment variables at startup
const requiredEnvVars = [
  'GETTING_ALL_DRIVERS_URL',
  'GETTING_DISPATCHERS_URL',
  'GETTING_TOKEN_URL',
  'DB_HOST',
  'DB_USER',
  'DB_NAME',
  'DB_PASSWORD',
  'DITAT_APPLICATION_ROLE',
  'DITAT_ACCOUNT_ID',
  'DITAT_AUTHORIZATION',
];

for (const varName of requiredEnvVars) {
  if (!process.env[varName]) {
    console.error(`❌ Missing required environment variable: ${varName}`);
    process.exit(1);
  }
}

const {
  GETTING_ALL_DRIVERS_URL,
  GETTING_DISPATCHERS_URL,
  GETTING_TOKEN_URL,
  DB_HOST,
  DB_USER,
  DB_NAME,
  DB_PASSWORD,
  DITAT_APPLICATION_ROLE,
  DITAT_ACCOUNT_ID,
  DITAT_AUTHORIZATION,
} = process.env;

const dispatcherIdAndName = {
  12: "Marko",
  28: "Mario",
  53: "Paul",
  57: "Milos",
  65: "Aleks",
  70: "Luka",
  72: "Adrian",
  78: "David",
  79: "Kevin",
  80: "Monte",
  81: "Austin",
};

const dispatcherIDs = [
  12, 28, 53, 57, 65, 70, 72, 78, 79, 80, 81
];

// Create a MySQL connection pool
const pool = mysql.createPool({
  host: DB_HOST,
  user: DB_USER,
  password: DB_PASSWORD,
  database: DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  ssl: {
    ca: fs.readFileSync(path.join(__dirname, 'client_certification', 'server-ca.pem')),
    cert: fs.readFileSync(path.join(__dirname, 'client_certification', 'client-cert.pem')),
    key: fs.readFileSync(path.join(__dirname, 'client_certification', 'client-key.pem')),
  }
});

// Get API token
async function getApiToken() {
  console.log("---------------------- Getting API token -------------------");
  const headers = {
    "Ditat-Application-Role": DITAT_APPLICATION_ROLE,
    "ditat-account-id": DITAT_ACCOUNT_ID,
    "Authorization": DITAT_AUTHORIZATION,
  };
  try {
    const { data } = await axios.post(GETTING_TOKEN_URL, {}, { headers });
    if (data) {
      console.log(`✔✔✔ API token= ${JSON.stringify(data)}`);
      return data;
    }
  } catch (error) {
    console.error("❌❌❌ Error getting API token:", error.message);
    throw error;
  }
  return null;
}

// Fetch all drivers data
async function fetchDrivers(apiToken) {
  console.log("---------------------- Getting drivers data ----------------------");
  const headers = {
    Authorization: `Ditat-Token ${apiToken}`,
  };
  const body = {
    filterItems: [
      {
        columnName: "driverId",
        filterType: 5,
        filterFromValue: "",
      },
    ],
  };
  try {
    const { data } = await axios.post(GETTING_ALL_DRIVERS_URL, body, { headers });
    if (data) {
      console.log("✔✔✔ Fetching drivers data Success");
      return data?.data?.data || [];
    }
  } catch (error) {
    console.error("❌❌❌ Error fetching drivers:", error.message);
    throw error;
  }
  return [];
}

// Fetch All Dispatchers data
async function getAllDispatchersData(apiToken) {
  console.log("---------------------- Getting Dispatchers ---------------------");
  const driverAndDispatcher = {};

  for (const individualDispatcher of dispatcherIDs) {
    // console.log(`✔✔✔ Retrieving ${dispatcherIdAndName[individualDispatcher]}'s drivers`);
    const each_dispatcher_url = `${GETTING_DISPATCHERS_URL}/${individualDispatcher}/item`;
    console.log(`✔✔✔ ${dispatcherIdAndName[individualDispatcher]}'s API calling url=${each_dispatcher_url}`);

    const headers = {
      Authorization: `Ditat-Token ${apiToken}`
    };

    try {
      const { data } = await axios.get(each_dispatcher_url, { headers });
      const oneDispatcherDriversInformation = data.data
      oneDispatcherDriversInformation.forEach(element => {
        let driver = element.recordId.trim();
        driverAndDispatcher[driver] = dispatcherIdAndName[individualDispatcher];
      });
    } catch (error) {
      console.log("❌❌❌ Error getting Dispatchers data:", error.message);
      throw error;
    }
  }
  return driverAndDispatcher;
}

//  Upsert drivers into DB
async function upsertDrivers(drivers) {
  let connection;
  try {
    connection = await pool.getConnection();
    console.log("Database connection success");

    const sql = `
    INSERT INTO drivers (
      driverId, status, firstName, lastName, truckId, phoneNumber, email, hiredOn, updatedOn,
      companyId, dispatcher, firstLanguage, secondLanguage,
      globalDnd, safetyCall, safetyMessage, hosSupport,
      maintainanceCall, maintainanceMessage,
      dispatchCall, dispatchMessage,
      accountCall, accountMessage
    )
    VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    ON DUPLICATE KEY UPDATE
      status = VALUES(status),
      firstName = VALUES(firstName),
      lastName = VALUES(lastName),
      truckId = VALUES(truckId),
      phoneNumber = VALUES(phoneNumber),
      email = VALUES(email),
      updatedOn = VALUES(updatedOn),
      companyId = VALUES(companyId),
      dispatcher = VALUES(dispatcher),
      firstLanguage = VALUES(firstLanguage),
      secondLanguage = VALUES(secondLanguage),
      globalDnd = VALUES(globalDnd),
      safetyCall = VALUES(safetyCall),
      safetyMessage = VALUES(safetyMessage),
      hosSupport = VALUES(hosSupport),
      maintainanceCall = VALUES(maintainanceCall),
      maintainanceMessage = VALUES(maintainanceMessage),
      dispatchCall = VALUES(dispatchCall),
      dispatchMessage = VALUES(dispatchMessage),
      accountCall = VALUES(accountCall),
      accountMessage = VALUES(accountMessage)
  `;

    for (const driver of drivers) {
      const params = [
        driver["driverId"], driver["status"], driver["firstName"], driver["lastName"], driver["truckId"],
        driver["phoneNumber"], driver["email"], driver["hiredOn"], new Date(), driver["companyId"],
        driver["dispatcher"], driver["firstLanguage"], driver["secondLanguage"], driver["globalDnd"],
        driver["safetyCall"], driver["safetyMessage"], driver["hosSupport"], driver["maintainanceCall"],
        driver["maintainanceMessage"], driver["dispatchCall"], driver["dispatchMessage"],
        driver["accountCall"], driver["accountMessage"]
      ];
      await connection.execute(sql, params);
      console.log(`♻♻♻ Driver ${driver["driverId"]} data processing success`);
    }
    console.log(`[${new Date().toISOString()}] Upsert complete!`);

  } catch (error) {
    console.error("❌❌❌Error during upsert:", error.message);
  } finally {
    if (connection) await connection.release();
  }
}

// Fetch and upsert process
async function fetchAndUpsertDrivers() {
  try {
    console.log(`[${new Date().toISOString()}] Starting fetch and upsert...`);
    const apiToken = await getApiToken();
    const rawDrivers = await fetchDrivers(apiToken);
    const driverAndDispatcher = await getAllDispatchersData(apiToken);

    const drivers = rawDrivers.map((d) => {
      let convertedDriverId = d.driverId;
      let dispatcher = driverAndDispatcher[convertedDriverId];
      return (
        {
          'driverId': convertedDriverId,
          'status': d.status,
          'firstName': d.firstName,
          'lastName': d.lastName,
          'truckId': d.truckId,
          'phoneNumber': d.phoneNumber,
          'email': d.emailAddress,
          'hiredOn': d.hiredOn,
          'updatedOn': d.updatedOn,
          'companyId': d.companyId,
          'dispatcher': dispatcher,
          // Add other fields as needed, ensure all required DB fields are mapped
        }
      )
    });

    await upsertDrivers(drivers);
    return drivers;
  } catch (error) {
    console.error(`[${new Date().toISOString()}] Error:`, error.message);
  }
}

// Function to schedule periodic updates
function scheduleDriverUpdates(intervalMs) {
  console.log(`Scheduling driver data update every ${intervalMs / (60 * 60 * 1000)} hours.`);
  let intervalId = setInterval(async () => {
    console.log('Scheduled update triggered.');
    await fetchAndUpsertDrivers();
  }, intervalMs);

  // Graceful shutdown
  const shutdown = async () => {
    console.log('Shutting down, clearing scheduled updates.');
    clearInterval(intervalId);
    await pool.end();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

// Run once at startup, then every 6 hours
(async () => {
  await fetchAndUpsertDrivers();
  scheduleDriverUpdates(6 * 60 * 60 * 1000); // 6 hours
})();
