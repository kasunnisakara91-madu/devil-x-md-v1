const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const yts = require('yt-search');
const FileType = require('file-type');
const { MongoClient } = require('mongodb');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
ffmpeg.setFfmpegPath(ffmpegInstaller.path);

const {
    default: makeWASocket,
    useMultiFileAuthState,
    delay,
    getContentType,
    jidNormalizedUser,
    downloadContentFromMessage,
    DisconnectReason,
    Browsers,
    fetchLatestWaWebVersion
} = require('@whiskeysockets/baileys');

// ==================== CONFIG ====================

const BOT_NAME_FANCY = 'ÐΣVłŁ-X-MÐ';

const config = {
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'false',
  AUTO_LIKE_EMOJI: ['💙', '🩷', '💜', '🤎', '🧡', '🩵', '💛', '🩶', '♥️', '💗', '❤️‍🔥'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  API_YTMP3_URL: 'https://ytmp3-download-api.vercel.app' ,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/KHjLmF0otPdJ65wQImHLEF?s',
  RCD_IMAGE_PATH: 'https://litter.catbox.moe/qb9z0z.jpg',
  NEWSLETTER_JID: [
      '120363428670000697@newsletter','120363161833328112@newsletter'],
  OTP_EXPIRY: 300000,
  OWNER_NUMBER: process.env.OWNER_NUMBER ? process.env.OWNER_NUMBER.split(',') : ['94783081889'],
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029VbDH0dj7T8bXPXQFoM0B',
  BOT_NAME: '© ÐΣVłŁ-X-MÐ',
  BOT_VERSION: '1.0.0 ULTRA',
  OWNER_NAME: 'DINIDU',
  IMAGE_PATH: 'https://litter.catbox.moe/qb9z0z.jpg',
  BOT_FOOTER: '> *© ÐΣVłŁ-X-MÐ*',
  
  // Default settings values
  DEFAULT_SETTINGS: {
    WORK_TYPE: 'public',
    AUTO_VIEW_STATUS: 'true',
    AUTO_REPLY: 'true',
    AUTO_VOICE: 'on',
    AUTO_STICKER: 'false',
    ANTI_BAD: 'false',
    ANTI_LINK: 'true',
    ANTI_BOT: 'false',
    PRESENCE: 'online',
    READ_COMMAND: 'true',
    AUTO_RECORDING: 'false',
    AUTO_TYPING: 'false',
    AUTO_LIKE_STATUS: 'true',
    BAD_NO_BLOCK: 'false',
    AI_CHAT: 'true',
    ANTI_CALL: 'off',
    WELCOME_GOODBYE: 'false',
    ANTI_DELETE: 'off',
    AUTO_TIKTOK: 'false',
    AUTO_NEWS: 'false',
    AUTO_REPLY_MODE: 'default',
    MOVIE_MODE: 'public'
  }
};

// ==================== MONGO SETUP ====================

// Config cache to avoid repeated database queries
const configCache = new Map();
const CONFIG_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://ransikavoice_db_user:Pv4nX6iyYaUPpg23@test.te0sgjd.mongodb.net/';
const MONGO_DB = process.env.MONGO_DB || 'MADUSHANKA_MD';

let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol;
let mongoInitialized = false;
let mongoInitPromise = null;

async function initMongo() {
  if (mongoInitialized && mongoClient) return;
  if (mongoInitPromise) return mongoInitPromise;
  
  mongoInitPromise = (async () => {
    try {
      if (mongoClient?.topology?.isConnected) return;
    } catch (e) { }
    
    mongoClient = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true, maxPoolSize: 10 });
    await mongoClient.connect();
    mongoDB = mongoClient.db(MONGO_DB);

    sessionsCol = mongoDB.collection('sessions');
    numbersCol = mongoDB.collection('numbers');
    adminsCol = mongoDB.collection('admins');
    newsletterCol = mongoDB.collection('newsletter_list');
    configsCol = mongoDB.collection('configs');
    newsletterReactsCol = mongoDB.collection('newsletter_reacts');

    await Promise.all([
      sessionsCol.createIndex({ number: 1 }, { unique: true }),
      numbersCol.createIndex({ number: 1 }, { unique: true }),
      newsletterCol.createIndex({ jid: 1 }, { unique: true }),
      newsletterReactsCol.createIndex({ jid: 1 }, { unique: true }),
      configsCol.createIndex({ number: 1 }, { unique: true })
    ]);
    
    mongoInitialized = true;
    console.log('✅ Mongo initialized and collections ready');
  })();
  
  return mongoInitPromise;
}

// ==================== Mongo Helpers ====================

async function saveCredsToMongo(number, creds, keys = null) {
  try {
    if (!sessionsCol || !mongoInitialized) await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = { number: sanitized, creds, keys, updatedAt: new Date() };
    await sessionsCol.updateOne({ number: sanitized }, { $set: doc }, { upsert: true });
  } catch (e) { console.error('saveCredsToMongo error:', e); }
}

async function loadCredsFromMongo(number) {
  try {
    if (!sessionsCol || !mongoInitialized) await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await sessionsCol.findOne({ number: sanitized });
    return doc || null;
  } catch (e) { console.error('loadCredsFromMongo error:', e); return null; }
}

async function removeSessionFromMongo(number) {
  try {
    if (!sessionsCol || !mongoInitialized) await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await sessionsCol.deleteOne({ number: sanitized });
  } catch (e) { console.error('removeSessionFromMongo error:', e); }
}

async function addNumberToMongo(number) {
  try {
    if (!numbersCol || !mongoInitialized) await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.updateOne({ number: sanitized }, { $set: { number: sanitized } }, { upsert: true });
  } catch (e) { console.error('addNumberToMongo', e); }
}

async function removeNumberFromMongo(number) {
  try {
    if (!numbersCol || !mongoInitialized) await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.deleteOne({ number: sanitized });
  } catch (e) { console.error('removeNumberFromMongo', e); }
}

async function getAllNumbersFromMongo() {
  try {
    if (!numbersCol || !mongoInitialized) await initMongo();
    const docs = await numbersCol.find({}).toArray();
    return docs.map(d => d.number);
  } catch (e) { console.error('getAllNumbersFromMongo', e); return []; }
}

async function loadAdminsFromMongo() {
  try {
    if (!adminsCol || !mongoInitialized) await initMongo();
    const docs = await adminsCol.find({}).toArray();
    return docs.map(d => d.jid || d.number).filter(Boolean);
  } catch (e) { console.error('loadAdminsFromMongo', e); return []; }
}

async function addAdminToMongo(jidOrNumber) {
  try {
    if (!adminsCol || !mongoInitialized) await initMongo();
    const doc = { jid: jidOrNumber };
    await adminsCol.updateOne({ jid: jidOrNumber }, { $set: doc }, { upsert: true });
  } catch (e) { console.error('addAdminToMongo', e); }
}

async function removeAdminFromMongo(jidOrNumber) {
  try {
    if (!adminsCol || !mongoInitialized) await initMongo();
    await adminsCol.deleteOne({ jid: jidOrNumber });
  } catch (e) { console.error('removeAdminFromMongo', e); }
}

async function addNewsletterToMongo(jid, emojis = []) {
  try {
    if (!newsletterCol || !mongoInitialized) await initMongo();
    const doc = { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() };
    await newsletterCol.updateOne({ jid }, { $set: doc }, { upsert: true });
  } catch (e) { console.error('addNewsletterToMongo', e); throw e; }
}

async function removeNewsletterFromMongo(jid) {
  try {
    if (!newsletterCol || !mongoInitialized) await initMongo();
    await newsletterCol.deleteOne({ jid });
  } catch (e) { console.error('removeNewsletterFromMongo', e); throw e; }
}

async function listNewslettersFromMongo() {
  try {
    if (!newsletterCol || !mongoInitialized) await initMongo();
    const docs = await newsletterCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewslettersFromMongo', e); return []; }
}

async function listNewsletterReactsFromMongo() {
  try {
    if (!newsletterReactsCol || !mongoInitialized) await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewsletterReactsFromMongo', e); return []; }
}

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  try {
    if (!mongoDB || !mongoInitialized) await initMongo();
    const col = mongoDB.collection('newsletter_reactions_log');
    const doc = { jid, messageId, emoji, sessionNumber, ts: new Date() };
    await col.insertOne(doc);
  } catch (e) { console.error('saveNewsletterReaction', e); }
}

async function setUserConfigInMongo(number, conf) {
  try {
    if (!configsCol || !mongoInitialized) await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne({ number: sanitized }, { $set: { number: sanitized, config: conf, updatedAt: new Date() } }, { upsert: true });
    // Invalidate cache
    configCache.delete(sanitized);
  } catch (e) { console.error('setUserConfigInMongo', e); }
}

async function loadUserConfigFromMongo(number) {
  try {
    const sanitized = number.replace(/[^0-9]/g, '');
    
    // Check cache first
    const cached = configCache.get(sanitized);
    if (cached && Date.now() - cached.time < CONFIG_CACHE_TTL) {
      return cached.config;
    }
    
    if (!configsCol || !mongoInitialized) await initMongo();
    const doc = await configsCol.findOne({ number: sanitized });
    const userConfig = doc ? doc.config : {};
    const result = { ...config.DEFAULT_SETTINGS, ...userConfig };
    
    // Cache the result
    configCache.set(sanitized, { config: result, time: Date.now() });
    return result;
  } catch (e) { console.error('loadUserConfigFromMongo', e); return { ...config.DEFAULT_SETTINGS }; }
}

// ==================== Basic Utils ====================

function formatMessage(title, content, footer) {
  return `${title}\n\n${content}\n\n> *${footer}*`;
}
function generateOTP() { return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp() { return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();
const socketCreationTime = new Map();
const otpStore = new Map();

// ==================== Helpers ====================

async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      let errorMessage = error.message || 'Unknown error';
      if (error.message && error.message.includes('not-authorized')) errorMessage = 'Bot not authorized';
      else if (error.message && error.message.includes('conflict')) errorMessage = 'Already a member';
      else if (error.message && error.message.includes('gone')) errorMessage = 'Invite invalid/expired';
      if (retries === 0) return { status: 'failed', error: errorMessage };
      await delay(2000 * (config.MAX_RETRIES - retries));
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

async function sendAdminConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  const admins = await loadAdminsFromMongo();
  const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
  const botName = sessionConfig.botName || BOT_NAME_FANCY;
  const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
  const caption = formatMessage(botName, `*📞 𝗡ᴜᴍʙᴇʀ:* ${number}\n*🍁 𝗦ᴛᴀᴛᴜꜱ:* ${groupStatus}\n*🕒 𝗖ᴏɴɴᴇᴄᴛᴇᴅ 𝗔ᴛ:* ${getSriLankaTimestamp()}`, botName);
  for (const admin of admins) {
    try {
      const to = admin.includes('@') ? admin : `${admin}@s.whatsapp.net`;
      if (String(image).startsWith('http')) {
        await socket.sendMessage(to, { image: { url: image }, caption });
      } else {
        try {
          const buf = fs.readFileSync(image);
          await socket.sendMessage(to, { image: buf, caption });
        } catch (e) {
          await socket.sendMessage(to, { image: { url: config.RCD_IMAGE_PATH }, caption });
        }
      }
    } catch (err) {
      console.error('Failed to send connect message to admin', admin, err?.message || err);
    }
  }
}

async function sendOwnerConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  try {
    const ownerNumbers = config.OWNER_NUMBER.map(num => `${num.replace(/[^0-9]/g, '')}@s.whatsapp.net`);
    const activeCount = activeSockets.size;
    const botName = sessionConfig.botName || BOT_NAME_FANCY;
    const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
    const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
    const caption = formatMessage(`*🥷 𝗢ᴡɴᴇʀ 𝗖ᴏɴᴛᴀᴄᴛ: ${botName}*`, 
      `*📞 𝗡ᴜᴍʙᴇʀ:* ${number}\n*🍁 𝗦ᴛᴀᴛᴜꜱ:* ${groupStatus}\n*🕒 𝗖ᴏɴɴᴇᴄᴛᴇᴅ 𝗔ᴛ:* ${getSriLankaTimestamp()}\n\n*🔢 𝗔ᴄᴛɪᴠᴇ 𝗦ᴇꜱꜱɪᴏɴꜱ:* ${activeCount}`, 
      botName);

    for (const ownerJid of ownerNumbers) {
      if (String(image).startsWith('http')) {
        await socket.sendMessage(ownerJid, { image: { url: image }, caption });
      } else {
        try {
          const buf = fs.readFileSync(image);
          await socket.sendMessage(ownerJid, { image: buf, caption });
        } catch (e) {
          await socket.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
        }
      }
    }
  } catch (err) { console.error('Failed to send owner connect message:', err); }
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = formatMessage(`*🔐 𝐎𝚃𝙿 𝐕𝙴𝚁𝙸𝙵𝙸𝙲𝙰𝚃𝙸𝙾𝙽 — ${BOT_NAME_FANCY}*`, `*𝐘𝙾𝚄𝚁 𝐎𝚃𝙿 𝐅𝙾𝚁 𝐂𝙾𝙽𝙵𝙸𝙶 𝐔𝙿𝙳𝙰𝚃𝙴 𝐈𝚂:* *${otp}*\n𝐓𝙷𝙸𝚂 𝐎𝚃𝙿 𝐖𝙸𝙻𝙻 𝐄𝚇𝙿𝙸𝚁𝙴 𝐈𝙽 5 𝐌𝙸𝙽𝚄𝚃𝙴𝚂.\n\n*𝐍𝚄𝙼𝙱𝙴𝚁:* ${number}`, BOT_NAME_FANCY);
  try { await socket.sendMessage(userJid, { text: message }); console.log(`OTP ${otp} sent to ${number}`); }
  catch (error) { console.error(`Failed to send OTP to ${number}:`, error); throw error; }
}

// ==================== Handlers ====================

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;

    try {
      const followedDocs = await listNewslettersFromMongo();
      const reactConfigs = await listNewsletterReactsFromMongo();
      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);

      const followedMap = new Map(followedDocs.map(d => [d.jid, d]));
      if (!followedMap.has(jid) && !reactMap.has(jid)) return;

      let emojis = reactMap.get(jid) || null;
      if ((!emojis || emojis.length === 0) && followedMap.has(jid)) {
        emojis = (followedMap.get(jid).emojis || []);
      }
      if (!emojis || emojis.length === 0) emojis = config.AUTO_LIKE_EMOJI;

      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;

      let retries = 3;
      while (retries-- > 0) {
        try {
          if (typeof socket.newsletterReactMessage === 'function') {
            await socket.newsletterReactMessage(jid, messageId.toString(), emoji);
          } else {
            await socket.sendMessage(jid, { react: { text: emoji, key: message.key } });
          }
          await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null);
          break;
        } catch (err) {
          console.warn(`Reaction attempt failed (${3 - retries}/3):`, err?.message || err);
          await delay(1200);
        }
      }

    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message || error);
    }
  });
}

async function setupStatusHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;

    try {
      let userEmojis = config.AUTO_LIKE_EMOJI;
      let autoViewStatus = config.AUTO_VIEW_STATUS;
      let autoLikeStatus = config.AUTO_LIKE_STATUS;
      let autoRecording = config.AUTO_RECORDING;

      if (sessionNumber) {
        const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};

        if (userConfig.AUTO_LIKE_EMOJI && Array.isArray(userConfig.AUTO_LIKE_EMOJI) && userConfig.AUTO_LIKE_EMOJI.length > 0) {
          userEmojis = userConfig.AUTO_LIKE_EMOJI;
        }
        if (userConfig.AUTO_VIEW_STATUS !== undefined) autoViewStatus = userConfig.AUTO_VIEW_STATUS;
        if (userConfig.AUTO_LIKE_STATUS !== undefined) autoLikeStatus = userConfig.AUTO_LIKE_STATUS;
        if (userConfig.AUTO_RECORDING !== undefined) autoRecording = userConfig.AUTO_RECORDING;
      }

      if (autoRecording === 'true') {
        await socket.sendPresenceUpdate("recording", message.key.remoteJid);
      }

      if (autoViewStatus === 'true') {
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try {
            await socket.readMessages([message.key]);
            break;
          } catch (error) {
            retries--;
            await delay(1000 * (config.MAX_RETRIES - retries));
            if (retries === 0) throw error;
          }
        }
      }

      if (autoLikeStatus === 'true') {
        const randomEmoji = userEmojis[Math.floor(Math.random() * userEmojis.length)];
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try {
            await socket.sendMessage(message.key.remoteJid, {
              react: { text: randomEmoji, key: message.key }
            }, { statusJidList: [message.key.participant] });
            break;
          } catch (error) {
            retries--;
            await delay(1000 * (config.MAX_RETRIES - retries));
            if (retries === 0) throw error;
          }
        }
      }

    } catch (error) {
      console.error('Status handler error:', error);
    }
  });
}

async function handleMessageRevocation(socket, number) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    const messageKey = keys[0];
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    const mode = userConfig.ANTI_DELETE || 'off';
    if (mode === 'off') return;

    const isGroup = String(messageKey.remoteJid || '').endsWith('@g.us');
    if (mode === 'inbox' && isGroup) return;
    if (mode === 'group' && !isGroup) return;

    const userJid = jidNormalizedUser(socket.user.id);
    const deletionTime = getSriLankaTimestamp();
    const message = formatMessage('*🗑️ 𝗠ᴇꜱꜱᴀɢᴇ 𝗗ᴇʟᴇᴛᴇᴅ*', `A message was deleted from your chat.\n*📋 𝗙ʀᴏᴍ:* ${messageKey.remoteJid}\n*🍁 𝗗ᴇʟᴇᴛɪᴏɴ 𝗧ɪᴍᴇ:* ${deletionTime}`, BOT_NAME_FANCY);
    try { await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: message }); }
    catch (error) { console.error('Failed to send deletion notification:', error); }
  });
}

async function setupWelcomeGoodbye(socket, sessionNumber) {
  socket.ev.on('group-participants.update', async (update) => {
    try {
      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      if (userConfig.WELCOME_GOODBYE !== 'true') return;

      const groupId = update.id;
      const participants = update.participants || [];
      if (!participants.length) return;

      try {
        const groupMetadata = await socket.groupMetadata(groupId);
        const groupName = groupMetadata?.subject || "Our Group";
        const memberCount = groupMetadata?.participants?.length || 0;

        for (const participant of participants) {
          const userId = participant.split('@')[0];

          if (update.action === 'add') {
            const welcomeMsg = `
╭━━━〔 🌟 W E L C O M E 🌟 〕━━━⬣

👋 Hey *@${userId}* ✨
🎉 Welcome to *${groupName}*

╭━━━〔 💎 GROUP INFO 〕━━━⬣
┃ 👥 Members : ${memberCount}
┃ 🏷️ Status : New Member
╰━━━━━━━━━━━━━━⬣

╭━━━〔 📌 RULES 〕━━━⬣
┃ 🔹 Be respectful 🤝
┃ 🔹 No spam 🚫
┃ 🔹 Enjoy & stay active 💬
╰━━━━━━━━━━━━━━⬣

╭━━━〔 🌈 MESSAGE 〕━━━⬣
┃ 💖 We're happy to have you here!
┃ 🚀 Hope you enjoy your stay
╰━━━━━━━━━━━━━━⬣

╭━━━〔 ✨ ENJOY ✨ 〕━━━⬣
╰━━━━━━━━━━━━━━⬣
`;
            await socket.sendMessage(groupId, {
              image: { url: userConfig.logo || config.RCD_IMAGE_PATH },
              caption: welcomeMsg,
              mentions: [participant]
            });
          } else if (update.action === 'remove') {
            const goodbyeMsg = `
╭━━━〔 🌙 G O O D B Y E 🌙 〕━━━⬣

👋 Bye *@${userId}* 💔
🚪 You left *${groupName}*

╭━━━〔 📊 GROUP STATUS 〕━━━⬣
┃ 👥 Members Left : ${memberCount - 1}
┃ 🏷️ Status : Left Group
╰━━━━━━━━━━━━━━⬣

╭━━━〔 💔 MESSAGE 〕━━━⬣
┃ 😢 You will be missed here
┃ 🤍 Doors always open for you
╰━━━━━━━━━━━━━━⬣

╭━━━〔 🌌 TAKE CARE 🌌 〕━━━⬣
┃ 🌟 Stay safe & happy
┃ 💫 Hope to see you again
╰━━━━━━━━━━━━━━⬣
`;
            await socket.sendMessage(groupId, {
              image: { url: userConfig.logo || config.RCD_IMAGE_PATH },
              caption: goodbyeMsg,
              mentions: [participant]
            });
          }
        }
      } catch (metaErr) {
        console.error('Failed to get group metadata:', metaErr);
      }
    } catch (err) {
      console.error('WelcomeGoodbye error:', err);
    }
  });
}

async function setupCallRejection(socket, sessionNumber) {
  socket.ev.on('call', async (calls) => {
    try {
      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      if (userConfig.ANTI_CALL !== 'on') return;

      for (const call of calls) {
        if (call.status !== 'offer') continue;
        const id = call.id;
        const from = call.from;
        await socket.rejectCall(id, from);
        await socket.sendMessage(from, { text: '*🔕 Auto call rejection is enabled. Calls are automatically rejected.*' });
        const userJid = jidNormalizedUser(socket.user.id);
        const rejectionMessage = formatMessage('📞 CALL REJECTED', `Auto call rejection is active.\n\nCall from: ${from}\nTime: ${getSriLankaTimestamp()}`, BOT_NAME_FANCY);
        await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: rejectionMessage });
      }
    } catch (err) {
      console.error(`Call rejection error for ${sessionNumber}:`, err);
    }
  });
}

async function setupAutoMessageRead(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    const autoReadSetting = userConfig.READ_COMMAND || 'false';

    if (autoReadSetting !== 'true') return;

    let body = '';
    try {
      const type = getContentType(msg.message);
      const actualMsg = (type === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

      if (type === 'conversation') body = actualMsg.conversation || '';
      else if (type === 'extendedTextMessage') body = actualMsg.extendedTextMessage?.text || '';
    } catch (e) { body = ''; }

    const prefix = userConfig.PREFIX || config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);

    if (isCmd) {
      try { await socket.readMessages([msg.key]); } catch (error) { console.warn('Failed to read command message:', error?.message); }
    }
  });
}

async function setupMessageHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    try {
      let autoTyping = false;
      let autoRecording = false;

      if (sessionNumber) {
        const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};
        if (userConfig.AUTO_TYPING === 'true') autoTyping = true;
        if (userConfig.AUTO_RECORDING === 'true') autoRecording = true;
      }

      if (autoTyping) {
        try {
          await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
          setTimeout(async () => { try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) { } }, 3000);
        } catch (e) { console.error('Auto typing error:', e); }
      }

      if (autoRecording) {
        try {
          await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
          setTimeout(async () => { try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) { } }, 3000);
        } catch (e) { console.error('Auto recording error:', e); }
      }
    } catch (error) {
      console.error('Message handler error:', error);
    }
  });
}

// ==================== AUTO VOICE HANDLER ====================

async function setupAutoVoice(socket, sessionNumber) {
  const voiceReplies = {
    'gm': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/gm.ogg',
    'good morning': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/gm.ogg',
    'gn': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/gn.mp3',
    'good night': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/good%20night.mp3',
    'hi': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/hi%20lassana%20lamayo.ogg',
    'hey': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/hi%20lassana%20lamayo.ogg',
    'hello': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/hi%20lassana%20lamayo.ogg',
    'helo': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/hi%20lassana%20lamayo.ogg',
    'hy': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/hi%20lassana%20lamayo.ogg',
    'bye': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bye%20lassana%20lamayo.ogg',
    'hm': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bye%20lassana%20lamayo.ogg',
    'mk': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/mk.ogg',
    'mokada karanne': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/mk.ogg',
    'adareyi': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/adarei.mp3',
    'ආදරෙයි': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/adarei.mp3',
    'love you': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/adarei.mp3',
    'i love you': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/adarei.mp3',
    'ha ha': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/hako.mp3',
    'hako': 'https://github.com/TECH-HORIZON-SCHOOL-OFFICIAL/PROJECT_HORIZON/raw/refs/heads/main/voice%20clips/hako.mp3',
    'bot': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/hi%20lassana%20lamayo.ogg',
    'hutta': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'pakaya': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'ponnaya': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'utta': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'ponz': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'wesigeputha': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'huttigeputha': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'huththa': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg',
    'huththigeputha': 'https://raw.githubusercontent.com/dct-dula/database/48c3556468d3f7f81ce6b4ec974a83f2aea1b467/voice/bad%20words.ogg'
  };

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.fromMe) return;
    if (msg.key.remoteJid === 'status@broadcast') return;

    try {
      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      if (userConfig.AUTO_VOICE === 'off') return;

      const msgType = getContentType(msg.message);
      let body = '';
      if (msgType === 'conversation') body = msg.message.conversation || '';
      else if (msgType === 'extendedTextMessage') body = msg.message.extendedTextMessage?.text || '';
      if (!body) return;

      const bodyLower = body.trim().toLowerCase();
      const voiceUrl = voiceReplies[bodyLower];
      if (!voiceUrl) return;

      try {
        const voiceResponse = await axios.get(voiceUrl, { responseType: 'arraybuffer' });
        const voiceBuffer = Buffer.from(voiceResponse.data);
        await socket.sendMessage(msg.key.remoteJid, {
          audio: voiceBuffer,
          mimetype: 'audio/ogg; codecs=opus',
          ptt: true
        }, { quoted: msg });
        console.log(`🎵 Auto voice sent for: ${bodyLower}`);
      } catch (voiceErr) {
        console.error('Auto voice send error:', voiceErr?.message || voiceErr);
      }
    } catch (err) {
      console.error('setupAutoVoice error:', err);
    }
  });
}

// ==================== AUTO REPLY HANDLER ====================

async function setupAutoReply(socket, sessionNumber) {
  const autoReplies = {
    'hi': '👋 *𝗛ᴇʏ!* 𝗧ʜᴀɴᴋꜱ ꜰᴏʀ ᴍᴇꜱꜱᴀɢɪɴɢ! 😊\n\n_𝘐 𝘢𝘮 𝘤𝘶𝘳𝘳𝘦𝘯𝘵𝘭𝘺 𝘣𝘶𝘴𝘺. 𝘐 𝘸𝘪𝘭𝘭 𝘳𝘦𝘱𝘭𝘺 𝘴𝘰𝘰𝘯!_\n\n> *© ÐΣVłŁ-X-MÐ*',
    'hey': '👋 *𝗛ᴇʏ!* 𝗧ʜᴀɴᴋꜱ ꜰᴏʀ ᴍᴇꜱꜱᴀɢɪɴɢ! 😊\n\n_𝘐 𝘢𝘮 𝘤𝘶𝘳𝘳𝘦𝘯𝘵𝘭𝘺 𝘣𝘶𝘴𝘺. 𝘐 𝘸𝘪𝘭𝘭 𝘳𝘦𝘱𝘭𝘺 𝘴𝘰𝘰𝘯!_\n\n> *© ÐΣVłŁ-X-MÐ*',
    'hello': '👋 *𝗛ᴇʟʟᴏ!* 𝗧ʜᴀɴᴋꜱ ꜰᴏʀ ʀᴇᴀᴄʜɪɴɢ ᴏᵁᴛ! 😊\n\n_𝘐 𝘢𝘮 𝘤𝘶𝘳𝘳𝘦𝘯𝘵𝘭𝘺 𝘣𝘶𝘴𝘺. 𝘐 𝘸𝘪𝘭𝘭 𝘳𝘦𝘱𝘭𝘺 𝘴𝘰𝘰𝘯!_\n\n> *© ÐΣVłŁ-X-MÐ*',
    'helo': '👋 *𝗛ᴇʟʟᴏ!* 𝗧ʜᴀɴᴋꜱ ꜰᴏʀ ʀᴇᴀᴄʜɪɴɢ ᴏᴜᴛ! 😊\n\n_𝘐 𝘢𝘮 𝘤𝘶𝘳𝘳𝘦𝘯𝘵𝘭𝘺 𝘣𝘶𝘴𝘺. 𝘐 𝘸𝘪𝘭𝘭 𝘳𝘦𝘱𝘭𝘺 𝘴𝘰𝘰𝘯!_\n\n> *© ÐΣVłŁ-X-MÐ*',
    'hy': '👋 *𝗛ᴇʏ!* 𝗧ʜᴀɴᴋꜱ ꜰᴏʀ ᴍᴇꜱꜱᴀɢɪɴɢ! 😊\n\n_𝘐 𝘢𝘮 𝘤𝘶𝘳𝘳𝘦𝘯𝘵𝘭𝘺 𝘣𝘶𝘴𝘺. 𝘐 𝘸𝘪𝘭𝘭 𝘳𝘦𝘱𝘭𝘺 𝘴𝘰𝘰𝘯!_\n\n> *© ÐΣVłŁ-X-MÐ*',
    'gm': '🌅 *𝗚ᴏᴏᴅ 𝗠ᴏʀɴɪɴɢ!* ☀️\n\n_𝘏𝘢𝘷𝘦 𝘢 𝘣𝘦𝘢𝘶𝘵𝘪𝘧𝘶𝘭 𝘥𝘢𝘺 𝘢𝘩𝘦𝘢𝘥!_ 🌸\n\n> *© ÐΣVłŁ-X-MÐ*',
    'good morning': '🌅 *𝗚ᴏᴏᴅ 𝗠ᴏʀɴɪɴɢ!* ☀️\n\n_𝘏𝘢𝘷𝘦 𝘢 𝘣𝘦𝘢𝘶𝘵𝘪𝘧𝘶𝘭 𝘥𝘢𝘺 𝘢𝘩𝘦𝘢𝘥!_ 🌸\n\n> *© ÐΣVłŁ-X-MÐ*',
    'gn': '🌙 *𝗚ᴏᴏᴅ 𝗡ɪɢʜᴛ!* 😴\n\n_𝘚𝘸𝘦𝘦𝘵 𝘥𝘳𝘦𝘢𝘮𝘴!_ 💤\n\n> *© ÐΣVłŁ-X-MÐ*',
    'good night': '🌙 *𝗚ᴏᴏᴅ 𝗡ɪɢʜᴛ!* 😴\n\n_𝘚𝘸𝘦𝘦𝘵 𝘥𝘳𝘦𝘢𝘮𝘴!_ 💤\n\n> *© ÐΣVłŁ-X-MÐ*',
    'bye': '👋 *𝗚ᴏᴏᴅʙʏᴇ!* 🌸\n\n_𝘛𝘢𝘬𝘦 𝘤𝘢𝘳𝘦 & 𝘴𝘵𝘢𝘺 𝘴𝘢𝘧𝘦!_ 💙\n\n> *© ÐΣVłŁ-X-MÐ*',
    'ok': '✅ *𝗢𝗸!* 😊\n\n> *© ÐΣVłŁ-X-MÐ*',
    'okay': '✅ *𝗢𝗸𝗮𝘆!* 😊\n\n> *© ÐΣVłŁ-X-MÐ*',
    'thanks': '🙏 *𝗧ʜᴀɴᴋ 𝘆ᴏᴜ!* 😊 𝗠𝘆 ᴘʟᴇᴀꜱᴜʀᴇ! 💙\n\n> *© ÐΣVłŁ-X-MÐ*',
    'thank you': '🙏 *𝗬𝗼𝘂 ᴀʀᴇ ᴡᴇʟᴄᴏᴍᴇ!* 😊 𝗔ɴʏᵗɪᴍᴇ! 💙\n\n> *© ÐΣVłŁ-X-MÐ*',
    'love you': '❤️ *𝗟ᴏᴠᴇ 𝘆ᴏᴜ ᴛᴏᴏ!* 😘\n\n> *© ÐΣVłŁ-X-MÐ*',
    'i love you': '❤️ *𝗟ᴏᴠᴇ 𝘆ᴏᴜ ᴛᴏᴏ!* 😘\n\n> *© ÐΣVłŁ-X-MÐ*',
    'adareyi': '❤️ *𝗔ᴅᴀʀᴇʏɪ!* 😘\n\n> *© ÐΣVłŁ-X-MÐ*',
    'how are you': '😊 *𝗜 ᴀᴍ ᴅᴏɪɴɢ ɢʀᴇᴀᴛ! 𝗧ʜᴀɴᴋꜱ ꜰᴏʀ ᴀꜱᴋɪɴɢ!* 💙\n\n> *© ÐΣVłŁ-X-MÐ*',
    'hru': '😊 *𝗜 ᴀᴍ ᴅᴏɪɴɢ ɢʀᴇᴀᴛ! 𝗧ʜᴀɴᴋꜱ ꜰᴏʀ ᴀꜱᴋɪɴɢ!* 💙\n\n> *© ÐΣVłŁ-X-MÐ*',
    'bot': '🤖 *𝗬𝗲𝘀! 𝗜 ᴀᴍ ᴀ ʙᴏᴛ!*\n\n𝗧𝘆ᴘᴇ *.menu* ᴛᴏ ꜱᴇᴇ ᴀʟʟ ᴄᴏᴍᴍᴀɴᴅꜱ! ⚡\n\n> *© ÐΣVłŁ-X-MÐ*',
    'who are you': '🤖 *𝗜 ᴀᴍ ༺ ALONE X MD ꙰༻!*\n\n𝗔 ᴘᴏᴡᴇʀꜰᴜʟ 𝗪ʜᴀᴛꜱᴀᴘᴘ 𝗕ᴏᴛ! ⚡\n\nᵀʸᴾᵉ *.menu* ᵗᵒ ˢᵉᵉ ᵃˡˡ ᶜᵒᵐᵐᵃⁿᵈˢ!\n\n> *© ÐΣVłŁ-X-MÐ*'
  };

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.fromMe) return;
    if (msg.key.remoteJid === 'status@broadcast') return;
    const isGroup = (msg.key.remoteJid || '').endsWith('@g.us');
    if (isGroup) return;

    try {
      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      if (userConfig.AUTO_REPLY !== 'true') return;

      const msgType = getContentType(msg.message);
      let body = '';
      if (msgType === 'conversation') body = msg.message.conversation || '';
      else if (msgType === 'extendedTextMessage') body = msg.message.extendedTextMessage?.text || '';
      if (!body) return;

      const prefix = userConfig.PREFIX || config.PREFIX;
      if (body.startsWith(prefix)) return;

      const bodyLower = body.trim().toLowerCase();
      const replyText = autoReplies[bodyLower];
      if (!replyText) return;

      try {
        await socket.sendMessage(msg.key.remoteJid, { text: replyText }, { quoted: msg });
        console.log(`💬 Auto reply sent for: ${bodyLower}`);
      } catch (replyErr) {
        console.error('Auto reply send error:', replyErr?.message || replyErr);
      }
    } catch (err) {
      console.error('setupAutoReply error:', err);
    }
  });
}

// ==================== Cleanup Helper ====================

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch (e) { }
    activeSockets.delete(sanitized); socketCreationTime.delete(sanitized);
    try { await removeSessionFromMongo(sanitized); } catch (e) { }
    try { await removeNumberFromMongo(sanitized); } catch (e) { }
    try {
      const ownerNumbers = config.OWNER_NUMBER.map(num => `${num.replace(/[^0-9]/g, '')}@s.whatsapp.net`);
      const caption = formatMessage('*🥷 OWNER NOTICE — SESSION REMOVED*', `*𝐍umber:* ${sanitized}\n*𝐒ession 𝐑emoved 𝐃ue 𝐓o 𝐋ogout.*\n\n*𝐀ctive 𝐒essions 𝐍ow:* ${activeSockets.size}`, BOT_NAME_FANCY);
      for (const ownerJid of ownerNumbers) {
        if (socketInstance && socketInstance.sendMessage) await socketInstance.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
      }
    } catch (e) { }
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { console.error('deleteSessionAndCleanup error:', err); }
}

// ==================== Auto-Restart ====================

// number -> consecutive-reconnect-attempt count (module-level so it survives across calls)
const reconnectAttempts = new Map();
const MAX_RECONNECT_ATTEMPTS = 5;

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const sanitized = number.replace(/[^0-9]/g, '');
      const statusCode = lastDisconnect?.error?.output?.statusCode
        || lastDisconnect?.error?.statusCode
        || (lastDisconnect?.error && lastDisconnect.error.toString().includes('401') ? 401 : undefined);
      const isLoggedOut = statusCode === 401
        || (lastDisconnect?.error && lastDisconnect.error.code === 'AUTHENTICATION')
        || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
        || (lastDisconnect?.reason === DisconnectReason?.loggedOut);

      // statusCode 403 (banned) or a stream conflict/replaced session should NOT
      // be treated as a normal drop-and-retry — hammering reconnect after a ban
      // only makes things worse and can affect future re-pairing.
      const isBannedOrConflict = statusCode === 403
        || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('banned'));

      if (isLoggedOut || isBannedOrConflict) {
        console.log(`User ${number} logged out${isBannedOrConflict ? ' or banned' : ''}. Cleaning up (no auto-reconnect)...`);
        reconnectAttempts.delete(sanitized);
        try { await deleteSessionAndCleanup(number, socket); } catch (e) { console.error(e); }
        return;
      }

      const attempts = (reconnectAttempts.get(sanitized) || 0) + 1;
      if (attempts > MAX_RECONNECT_ATTEMPTS) {
        console.log(`Gave up reconnecting for ${number} after ${MAX_RECONNECT_ATTEMPTS} attempts.`);
        reconnectAttempts.delete(sanitized);
        try { await deleteSessionAndCleanup(number, socket); } catch (e) { console.error(e); }
        return;
      }
      reconnectAttempts.set(sanitized, attempts);

      // Exponential backoff (10s, 20s, 40s, 80s, 160s) instead of a fixed 10s loop.
      const backoffMs = 10000 * Math.pow(2, attempts - 1);
      console.log(`Connection closed for ${number} (not logout). Reconnect attempt ${attempts}/${MAX_RECONNECT_ATTEMPTS} in ${backoffMs / 1000}s...`);
      try {
        await delay(backoffMs);
        activeSockets.delete(sanitized);
        socketCreationTime.delete(sanitized);
        const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
        await EmpirePair(number, mockRes);
        reconnectAttempts.delete(sanitized); // reset on a successful new attempt kick-off
      } catch (e) { console.error('Reconnect attempt failed', e); }
    }
  });
}

// ==================== EmpirePair ====================

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber}`);
  if (!mongoInitialized) await initMongo().catch(() => { });

  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
      console.log('Prefilled creds from Mongo');
    }
  } catch (e) { console.warn('Prefill from Mongo failed', e); }

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);

  try {
    const { default: makeWASocket, fetchLatestBaileysVersion } = require("@whiskeysockets/baileys");

const { version } = await fetchLatestBaileysVersion();

const socket = makeWASocket({
    auth: state,
    version,
    printQRInTerminal: false,
    // ⚠️ "Ubuntu / Chrome / 22.04" is the default fingerprint copy-pasted across
    // thousands of public bot repos, which makes it an easy signature to flag.
    // Using a less common desktop fingerprint reduces (does not eliminate) that risk.
    browser: ["Windows", "Chrome", "10.0.22631"],
    connectTimeoutMs: 60000,
    keepAliveIntervalMs: 30000,
    generateHighQualityLinkPreview: true,
    markOnlineOnConnect: false
});


    socketCreationTime.set(sanitizedNumber, Date.now());

    setupStatusHandlers(socket, sanitizedNumber);
    setupCommandHandlers(socket, sanitizedNumber);
    setupMessageHandlers(socket, sanitizedNumber);
    setupAutoRestart(socket, sanitizedNumber);
    setupNewsletterHandlers(socket, sanitizedNumber);
    handleMessageRevocation(socket, sanitizedNumber);
    setupWelcomeGoodbye(socket, sanitizedNumber);
    setupAutoMessageRead(socket, sanitizedNumber);
    setupCallRejection(socket, sanitizedNumber);
    setupAutoVoice(socket, sanitizedNumber);
    setupAutoReply(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber); break; }
        catch (error) { retries--; await delay(2000 * (config.MAX_RETRIES - retries)); }
      }
      if (!res.headersSent) res.send({ code });
    }

    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        const credsPath = path.join(sessionPath, 'creds.json');
        if (!fs.existsSync(credsPath)) return;
        const fileStats = fs.statSync(credsPath);
        if (fileStats.size === 0) return;
        const fileContent = await fs.readFile(credsPath, 'utf8');
        const trimmedContent = fileContent.trim();
        if (!trimmedContent || trimmedContent === '{}' || trimmedContent === 'null') return;
        let credsObj;
        try { credsObj = JSON.parse(trimmedContent); } catch (e) { return; }
        if (!credsObj || typeof credsObj !== 'object') return;
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
        console.log('✅ Creds saved to MongoDB successfully');
      } catch (err) {
        console.error('Failed saving creds on creds.update:', err);
      }
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          const groupResult = await joinGroup(socket).catch(() => ({ status: 'failed', error: 'joinGroup not configured' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              const jid = doc.jid;
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); } catch (e) { }
            }
          } catch (e) { }

          activeSockets.set(sanitizedNumber, socket);
          const groupStatus = groupResult.status === 'success' ? 'Joined successfully' : `Failed to join group: ${groupResult.error}`;

          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = userConfig.logo || config.RCD_IMAGE_PATH;

          const initialCaption = formatMessage(useBotName,
            `*✅ 𝗦ᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ 𝗖ᴏɴɴᴇᴄᴛᴇᴅ ✅*\n\n*🔢 𝗡ᴜᴍʙᴇʀ :* ${sanitizedNumber}\n*📡 𝗖ᴏɴɴᴇᴄᴛɪɴɢ :* Wait few seconds`,
            useBotName
          );

          let sentMsg = null;
          try {
            if (String(useLogo).startsWith('http')) {
              sentMsg = await socket.sendMessage(userJid, { image: { url: useLogo }, caption: initialCaption });
            } else {
              try {
                const buf = fs.readFileSync(useLogo);
                sentMsg = await socket.sendMessage(userJid, { image: buf, caption: initialCaption });
              } catch (e) {
                sentMsg = await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: initialCaption });
              }
            }
          } catch (e) {
            try { sentMsg = await socket.sendMessage(userJid, { text: initialCaption }); } catch (e) { }
          }

          await delay(4000);

          const updatedCaption = formatMessage(
  useBotName,
  `╭━━━〔 𝗖𝗢𝗡𝗡𝗘𝗖𝗧𝗘𝗗〕━━━╮

┃ 🔢 𝗡𝘂𝗺𝗯𝗲𝗿   : ${sanitizedNumber}
┃ 🏷️ 𝗦𝘁𝗮𝘁𝘂𝘀   : ${groupStatus}
┃ 🕒 𝗧𝗶𝗺𝗲     : ${getSriLankaTimestamp()}

╰━━━━━━━━━━━━━━━━━━━━━━╯

✨𝗦𝘆𝘀𝘁𝗲𝗺 𝗶𝘀 𝗻𝗼𝘄 𝗼𝗻𝗹𝗶𝗻𝗲 & 𝗿𝗲𝗮𝗱𝘆!`,
  useBotName
);

          try {
            if (sentMsg && sentMsg.key) {
              try { await socket.sendMessage(userJid, { delete: sentMsg.key }); } catch (delErr) { }
            }
            try {
              if (String(useLogo).startsWith('http')) {
                await socket.sendMessage(userJid, { image: { url: useLogo }, caption: updatedCaption });
              } else {
                try {
                  const buf = fs.readFileSync(useLogo);
                  await socket.sendMessage(userJid, { image: buf, caption: updatedCaption });
                } catch (e) {
                  await socket.sendMessage(userJid, { text: updatedCaption });
                }
              }
            } catch (imgErr) {
              await socket.sendMessage(userJid, { text: updatedCaption });
            }
          } catch (e) { }

          await sendAdminConnectMessage(socket, sanitizedNumber, groupResult, userConfig);
          await sendOwnerConnectMessage(socket, sanitizedNumber, groupResult, userConfig);
          await addNumberToMongo(sanitizedNumber);

          await socket.sendMessage(userJid, { text: `✅ *${useBotName} is now online!*\n\nType *${config.PREFIX}menu* to see all available commands.\n\n_Thank you for using ÐΣVłŁ-X-MÐ!_` });

        } catch (e) {
          console.error('Connection open error:', e);
          try { exec(`pm2.restart ${process.env.PM2_NAME || 'DCT-NINJA-MD'}`); } catch (e) { }
        }
      }
      if (connection === 'close') {
        try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch (e) { }
      }
    });

    activeSockets.set(sanitizedNumber, socket);

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }
}

// ==================== COMPLETE COMMAND HANDLER WITH CASE TYPE ====================

function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message) return;
    
    if (msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    try {
      let body = '';
      const msgType = getContentType(msg.message);
      
      if (msgType === 'conversation') body = msg.message.conversation || '';
      else if (msgType === 'extendedTextMessage') body = msg.message.extendedTextMessage?.text || '';
      else if (msgType === 'imageMessage') body = msg.message.imageMessage?.caption || '';
      else if (msgType === 'videoMessage') body = msg.message.videoMessage?.caption || '';
      else if (msgType === 'buttonsResponseMessage') body = msg.message.buttonsResponseMessage?.selectedButtonId || '';
      else if (msgType === 'listResponseMessage') body = msg.message.listResponseMessage?.singleSelectReply?.selectedRowId || '';

      if (!body || typeof body !== 'string') return;
      
      const prefix = config.PREFIX;
      let fullCommand = '';
      if (body.startsWith(prefix)) {
        fullCommand = body.slice(prefix.length).trim();
      } else if (/^[0-9]+$/.test(body.trim())) {
        fullCommand = body.trim();
      } else {
        return;
      }
      const command = fullCommand.split(' ')[0].toLowerCase();
      const args = fullCommand.slice(command.length).trim().split(/\s+/).filter(Boolean);
      
      const from = msg.key.remoteJid;
      const sender = from;
      const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
      const senderNumber = (nowsender || '').split('@')[0];
      const isOwner = config.OWNER_NUMBER.some(owner => senderNumber === owner.replace(/[^0-9]/g, ''));
      const isGroup = from.endsWith("@g.us");
      
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};

      // Work type restrictions
      if (!isOwner) {
        const workType = userConfig.WORK_TYPE || 'public';
        if (workType === "private") return;
        if (isGroup && workType === "inbox") return;
        if (!isGroup && workType === "groups") return;
      }

      console.log(`📨 Command: ${command} from ${senderNumber}`);

      // Helper for quoted media
      async function downloadQuotedMedia(quoted) {
        if (!quoted) return null;
        const qTypes = ['imageMessage', 'videoMessage', 'audioMessage', 'documentMessage', 'stickerMessage'];
        const qType = qTypes.find(t => quoted[t]);
        if (!qType) return null;
        const messageType = qType.replace(/Message$/i, '').toLowerCase();
        const stream = await downloadContentFromMessage(quoted[qType], messageType);
        let buffer = Buffer.from([]);
        for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
        return { buffer, mime: quoted[qType].mimetype || '', caption: quoted[qType].caption || quoted[qType].fileName || '', ptt: quoted[qType].ptt || false, fileName: quoted[qType].fileName || '' };
      }

      // ==================== CASE TYPE COMMAND HANDLER ====================
      
      // Helper function to extract channel ID from WhatsApp channel link
      function extractChannelId(link) {
        if (!link) return null;
        
        // Handle different WhatsApp channel link formats
        const patterns = [
          /https?:\/\/(?:www\.)?whatsapp\.com\/channel\/([0-9]+)/i,
          /https?:\/\/chat\.whatsapp\.com\/channel\/([0-9]+)/i,
          /wa\.me\/channel\/([0-9]+)/i,
          /channel\/([0-9]+)/i,
          /([0-9]+)@newsletter/i
        ];
        
        for (const pattern of patterns) {
          const match = link.match(pattern);
          if (match && match[1]) {
            return `${match[1]}@newsletter`;
          }
        }
        
        // If it's already a JID format
        if (link.includes('@newsletter')) {
          return link;
        }
        
        return null;
      }
      
      switch(command) {
			  case 'menu': {
  try { await socket.sendMessage(sender, { react: { text: "🇱🇰", key: msg.key } }); } catch(e){}

  try {
    const startTime = socketCreationTime.get(number) || Date.now();
    const uptime = Math.floor((Date.now() - startTime) / 1000);
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);
    const seconds = Math.floor(uptime % 60);

    // load per-session config (logo, botName)
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; }
    catch(e){ console.warn('menu: failed to load config', e); userCfg = {}; }

    const title = userCfg.botName || 'ÐΣVłŁ-X-MÐ';

    // 🔹 Fake contact for Meta AI mention
    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_MENU"
        },
        message: {
            contactMessage: {
                displayName: title,
                vcard: `BEGIN:VCARD
VERSION:5.0
N:${title};;;;
FN:${title}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    const text = `
╭───❏ *BOT STATUS* ❏
│ 👽 *Bot Name*: ${title}
│ 👑 *Owner*: ${config.OWNER_NAME || 'DINIDU'}
│ 🏷️ *Version*: ${config.BOT_VERSION || '0.0001+'}
│ ☁️ *Platform*: ${process.env.PLATFORM || 'Senasuru✨'}
│ ⏳ *Uptime*: ${hours}h ${minutes}m ${seconds}s
╰───────────────❏

╭───❏ *𝗠𝗔𝗜𝗡 𝗠𝗘𝗡𝗨* ❏
│ 
│ 📥 *DOWNLOAD MENU*
│ ${config.PREFIX}download
│ 
│ 🎨 *CREATIVE MENU*  
│ ${config.PREFIX}creative
│
│ 🔧 *TOOLS MENU*
│ ${config.PREFIX}tools
│
│ ⚙️ *SETTINGS MENU*
│ ${config.PREFIX}settings
│
│ 👑 *OWNER MENU*
│ ${config.PREFIX}owner
│ 
│ ⚡ *PING TEST*
│ ${config.PREFIX}ping
│ 
│ 🤖 *BOT INFO*
│ ${config.PREFIX}alive
│
> © ${config.BOT_FOOTER || 'https://litter.catbox.moe/qb9z0z.jpg'}
`.trim();

    const buttons = [
      { buttonId: `${config.PREFIX}download`, buttonText: { displayText: "📥 DOWNLOAD" }, type: 1 },
      { buttonId: `${config.PREFIX}creative`, buttonText: { displayText: "🎨 CREATIVE" }, type: 1 },
      { buttonId: `${config.PREFIX}tools`, buttonText: { displayText: "🔧 TOOLS" }, type: 1 },
      { buttonId: `${config.PREFIX}settings`, buttonText: { displayText: "⚙️ SETTINGS" }, type: 1 },
      { buttonId: `${config.PREFIX}owner`, buttonText: { displayText: "👑 OWNER" }, type: 1 }
    ];

    const defaultImg = 'https://litter.catbox.moe/qb9z0z.jpg';
    const useLogo = userCfg.logo || defaultImg;

    // build image payload (url or buffer)
    let imagePayload;
    if (String(useLogo).startsWith('http')) imagePayload = { url: useLogo };
    else {
      try { imagePayload = fs.readFileSync(useLogo); } catch(e){ imagePayload = { url: defaultImg }; }
    }

    await socket.sendMessage(sender, {
      image: imagePayload,
      caption: text,
      footer: "ÐΣVłŁ-X-MÐ",
      buttons,
      headerType: 4
    }, { quoted: shonux });

  } catch (err) {
    console.error('menu command error:', err);
    try { await socket.sendMessage(sender, { text: '❌ Failed to show menu.' }, { quoted: msg }); } catch(e){}
  }
  break;
			  }
      case 'admin': {
  try { await socket.sendMessage(sender, { react: { text: "🍷", key: msg.key } }); } catch(e){}

  try {
    let userCfg = {};
    try { 
      if (number && typeof loadUserConfigFromMongo === 'function') 
        userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; 
    } catch(e){ 
      userCfg = {}; 
    }

    const title = userCfg.botName || '© 💚𝐁𝐄𝐒𝐓𝐈𝐄_𝐌𝐈𝐍𝐈😘';

    const shonux = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_FAKE_ID_OWNER"
      },
      message: {
        contactMessage: {
          displayName: title,
          vcard: `BEGIN:VCARD
VERSION:3.0
N:${title};;;;
FN:${title}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
        }
      }
    };

    const text = `
*╭─「𝐌𝐘 𝐈nfo」 ──●●➤*
*✘ 𝘕𝘢𝘮𝘦 =* *MADUSANKA *
*✘ 𝘈𝘨𝘦 =* *17*
*✘ 𝘕𝘣 =* *+94783731694*
*╰──────────●●➤*
> *💚𝐁𝐄𝐒𝐓𝐈𝐄_𝐌𝐈𝐍𝐈😘 𝐁ᴏᴛ*
`.trim();

    await socket.sendMessage(sender, {
      text,
      footer: "🥷 𝘖𝘸𝘯𝘦𝘳 𝘐𝘯𝘧𝘰𝘳𝘮𝘢𝘵𝘪𝘰𝘯"
    }, { quoted: shonux });

  } catch (err) {
    console.error('owner command error:', err);
    try { 
      await socket.sendMessage(sender, { text: '❌ Failed to show owner info.' }, { quoted: msg }); 
    } catch(e){}
  }
  break;
			  }
			  
      case 'song': {

const q = args.join(' ');
if (!q) return reply("🎵 Song name එකක් දෙන්න\n\nExample: .song lelena");


try {

await reply("⏳ Searching song...");


let video;
let ytUrl = q;


if (!/^https?:\/\//i.test(q)) {

const search = await yts(q);

video = search.videos[0];

if (!video) return reply("❌ Song not found");

ytUrl = video.url;

}


const API_KEY = "chama_api_23c3e7ffb034f25cf474f6d7ac266f9b";


const api = 
`https://chama-movie-api.koyeb.app/api/v1/youtube/mp3?url=${encodeURIComponent(ytUrl)}&quality=320kbps&source=auto&api_key=${API_KEY}`;


const {data} = await axios.get(api);



if(!data || !data.status)
return reply("❌ API error");



const song = data.data || {};

const title = song.title || video?.title || "Unknown";

const thumb = song.thumbnail || video?.thumbnail;

const download =
song.direct_url ||
data.download?.url;



if(!download)
return reply("❌ Download link not found");



// SONG DETAILS

await socket.sendMessage(sender,{

image:{
url:thumb
},

caption:`
╭━━━〔 🎵 SONG DOWNLOADER 〕━━━

🎧 *Title:* ${title}

⏱️ *Duration:* ${video?.timestamp || "N/A"}

⚡ *Quality:* 320kbps

╰━━━━━━━━━━━━━━━━━━
`


},{
quoted:msg
});



// AUTO SEND AUDIO

await socket.sendMessage(sender,{

audio:{
url:download
},

mimetype:"audio/mpeg",

fileName:`${title}.mp3`

},{

quoted:msg

});


} catch(e){

console.log(e);

reply("❌ Error : "+e.message);

}


break;


			  

												 }
												 
      case 'moviesublk':             
case 'msublk': {
    const DEFAULT_FOOTER = `\n\n> 🎭 ÐΣVłŁ-X-MÐ🎭\n> 🧬 ᴘᴏᴡᴇʀᴇᴅ ʙʏ ÐΣVłŁ-X-MÐ`;

    if (!args.length) {
        await socket.sendMessage(sender, {
            text: `*❪ ERROR ❫*\n\n⚠️ *Invalid Usage!*\n\n🎬 *Example:*
• .moviesublk spider man
• .msublk game of thrones\n\n📝 _Please provide the Movie_ _or TV Series name!_${DEFAULT_FOOTER}`
        }, { quoted: msg });
        break;
    }

    const query = args.join(' ');
    await socket.sendMessage(sender, { 
        text: `*❪ SEARCHING ❫*\n\n🔍 *Searching MovieSubLK...*\n⚡ _Please wait a moment._`
    });

    const API_BASE = "https://chama-movie-api.koyeb.app";
    const API_KEY = "chama_api_23c3e7ffb034f25cf474f6d7ac266f9b"; // ඔබේ API Key එක දාන්න
    const DEFAULT_IMAGE = "https://chama-movie-api.koyeb.app/logo.png";

    try {
        const searchResponse = await axios.get(`${API_BASE}/api/v1/movie/moviesublk/search?q=${encodeURIComponent(query)}&api_key=${API_KEY}`);
        const searchData = searchResponse.data;

        if (!searchData.status || !searchData.data || searchData.data.length === 0) {
            await socket.sendMessage(sender, {
                text: `*❪ NO RESULTS ❫*\n\n😞 *No Results Found!*\n\n🎬 *Query:* _${query}_\n💡 *Tip:* _Please check the spelling and try again!_${DEFAULT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        const results = searchData.data.slice(0, 25);
        let listText = `*❪ SEARCH RESULTS ❫*\n\n🎯 *Query:* _${query}_\n📊 *Results:* _${results.length} Items_\n\n*👇 SELECT A NUMBER 👇*\n\n`;

        results.forEach((item, index) => {
            const typeIcon = item.type === 'tvshows' ? '📺' : '🎥';
            const num = (index + 1) < 10 ? `0${index + 1}` : `${index + 1}`;
            listText += `*${num}* ➜ ${typeIcon} _${item.title.substring(0, 30)}_\n`;
        });

        listText += `${DEFAULT_FOOTER}`;
        
        const sentMsg = await socket.sendMessage(sender, { text: listText }, { quoted: msg });
        const messageID = sentMsg.key.id;

        const handleSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const messageType = replyMek.message.conversation || replyMek.message.extendedTextMessage?.text;
            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                const choice = parseInt(messageType) - 1;
                if (isNaN(choice) || choice < 0 || choice >= results.length) {
                    await socket.sendMessage(sender, {
                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${results.length}_\n📝 _Please reply with a valid number!_${DEFAULT_FOOTER}`
                    }, { quoted: replyMek });
                    return;
                }

                const selectedItem = results[choice];
                const isTvShow = selectedItem.type === 'tvshows';
                
                if (isTvShow) {
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n📺 *Fetching TV Series...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const tvShowResponse = await axios.get(`${API_BASE}/api/v1/movie/moviesublk/tv/info?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const tvShowData = tvShowResponse.data;

                        if (!tvShowData.status || !tvShowData.data) {
                            throw new Error('Failed to fetch TV show details');
                        }

                        const tvInfo = tvShowData.data;
                        
                        let tvDetailsText = `*❪ TV SERIES DETAILS ❫*\n\n📺 *${tvInfo.title}*\n⭐ 𝗜ᴍᴅ𝗯 ➜ ★ ${tvInfo.rating || 'N/A'}\n📅 𝗬ᴇᴀʀ ➜ ${tvInfo.year || 'N/A'}\n⏳ 𝗥ᴜɴᴛɪᴍᴇ ➜ ${tvInfo.duration || 'N/A'}\n🌍 𝗖ᴏᴜɴ𝘁𝗿ʏ ➜ ${tvInfo.country || 'N/A'}\n🎭 𝗚𝗲𝗻 genres ➜ ${tvInfo.genres ? tvInfo.genres.join(', ') : 'N/A'}\n📝 𝗦𝘁𝗼𝗿𝘆 ➜ ${tvInfo.story ? (tvInfo.story.length > 250 ? tvInfo.story.substring(0, 250) + '...' : tvInfo.story) : 'N/A'}\n🗿 𝗪ᴇʙ ➜ moviesublk.xyz\n ${DEFAULT_FOOTER}`;

                        const posterUrl = tvInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: posterUrl },
                            caption: tvDetailsText
                        }, { quoted: replyMek });

                        // AUTO DOWNLOAD ALL EPISODES
                        await socket.sendMessage(sender, { 
                            text: `*❪ DOWNLOAD EPISODES ❫*\n\n📺 *Series:* _${tvInfo.title}_
🎬 *Episodes:* _${tvInfo.episodes.length}_
⚡ _Starting download process..._	ext ${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        let successCount = 0;
                        let failCount = 0;

                        for (let i = 0; i < tvInfo.episodes.length; i++) {
                            const episode = tvInfo.episodes[i];
                            try {
                                await socket.sendMessage(sender, { 
                                    text: `*❪ DOWNLOADING ❫*\n\n🎥 *Episode:* _${episode.episode_name}_
📊 *Progress:* _${i + 1}/${tvInfo.episodes.length}_`
                                }, { quoted: replyMek });

                                const epDlRes = await axios.get(`${API_BASE}/api/v1/movie/moviesublk/tv/dl?q=${encodeURIComponent(episode.episode_url)}&api_key=${API_KEY}`);
                                const epDlData = epDlRes.data;

                                if (epDlData.status && epDlData.data && epDlData.data.length > 0) {
                                    const nonTelegramLinks = epDlData.data.filter(link => 
                                        link.link && !link.link.includes('t.me') && !link.link.includes('telegram')
                                    );
                                    const finalLinkObj = nonTelegramLinks[0] || epDlData.data[0];
                                    
                                    await socket.sendMessage(sender, {
                                        document: { url: finalLinkObj.link },
                                        mimetype: 'video/mp4',
                                        fileName: `${tvInfo.title} - ${episode.episode_name}.mp4`,
                                        caption: `*📺 ÐΣVłŁ-X-MÐ 📺*\n\n🎭 *Title:* ${tvInfo.title}\n📌 *Episode:* ${episode.episode_name}\n📊 *Quality:* Direct MP4\n\n${DEFAULT_FOOTER}`
                                    }, { quoted: replyMek });
                                    
                                    successCount++;
                                } else {
                                    failCount++;
                                }
                                
                                await new Promise(resolve => setTimeout(resolve, 2500));
                                
                            } catch (epError) {
                                console.error(`Error downloading episode:`, epError);
                                failCount++;
                            }
                        }
                        
                        await socket.sendMessage(sender, { 
                            text: `*❪ SUMMARY ❫*\n\n🎉 *Download Complete!*\n\n🎬 *Series:* _${tvInfo.title}_\n✅ *Success:* _${successCount} Episodes_\n❌ *Failed:* _${failCount} Episodes_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });

                        socket.ev.off('messages.upsert', handleSelection);
                        
                    } catch (tvShowError) {
                        console.error('TV Show error:', tvShowError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *TV Details Error!*\n🚫 _${tvShowError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                    
                } else {
                    // MOVIE FLOW
                    await socket.sendMessage(sender, { 
                        text: `*❪ FETCHING ❫*\n\n🎬 *Fetching Movie...*\n⚡ _Please wait..._`
                    }, { quoted: replyMek });

                    try {
                        const detailsResponse = await axios.get(`${API_BASE}/api/v1/movie/moviesublk/infodl?q=${encodeURIComponent(selectedItem.link)}&api_key=${API_KEY}`);
                        const detailsData = detailsResponse.data;

                        if (!detailsData.status || !detailsData.data) {
                            throw new Error('Failed to fetch details');
                        }

                        const movieInfo = detailsData.data;
                        const validDownloads = movieInfo.downloads || [];
                        
                        if (validDownloads.length === 0) {
                            await socket.sendMessage(sender, {
                                text: `*❪ NO DOWNLOADS ❫*\n\n⚠️ *No Downloads Found!*\n😞 _There are no downloads available for this movie!_${DEFAULT_FOOTER}`
                            }, { quoted: replyMek });
                            return;
                        }
                        
                        const movieDetailsText = `*❪ MOVIE DETAILS ❫*\n\n🎬 *${movieInfo.title}*\n⭐ 𝗜𝗠𝗗𝗕 ➜ ★ ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 𝗬𝗲𝗮𝗿 ➜ ${movieInfo.year || 'N/A'}\n⏳ 𝗗𝘂𝗿𝗮𝘁𝗶𝗼𝗻 ➜ ${movieInfo.duration || 'N/A'}\n🌍 𝗖ᴏᴜɴ𝘁𝗿ʏ ➜ ${movieInfo.country || 'N/A'}\n🎭 𝗚𝗲𝗻 genres ➜ ${movieInfo.genres ? movieInfo.genres.join(', ') : 'N/A'}\n🏷️  ➜ ${movieInfo.language || movieInfo.tag || 'N/A'}\n🎬  ➜ ${movieInfo.directors || movieInfo.director || 'N/A'}\n⭐  ➜ ${movieInfo.stars || 'N/A'}\n📝  ➜ ${movieInfo.story ? (movieInfo.story.length > 250 ? movieInfo.story.substring(0, 250) + '...' : movieInfo.story) : 'N/A'}\n🗿 𝗪ᴇʙ ➜ moviesublk.xyz\n ${DEFAULT_FOOTER}`;

                        const moviePosterUrl = movieInfo.image || selectedItem.image || DEFAULT_IMAGE;
                        await socket.sendMessage(sender, {
                            image: { url: moviePosterUrl },
                            caption: movieDetailsText
                        }, { quoted: replyMek });

                        const downloadOptionsText = `*❪ DOWNLOADS ❫*\n\n📥 *Select Quality:*\n\n${validDownloads.map((dl, i) => {
    const num = (i + 1) < 10 ? `0${i + 1}` : `${i + 1}`;
    const qualityIcon = (dl.quality || '').includes('1080') ? '🔥' : (dl.quality || '').includes('720') ? '💎' : '📱';
    return `*${num}* ➜ ${qualityIcon} _${dl.quality}_ 💾 _${dl.size || 'N/A'}_`;
}).join('\n')}\n\n*💬 REPLY TO DOWNLOAD 💬*\n📌 _Reply with the number_${DEFAULT_FOOTER}`;

                        const downloadOptionsMsg = await socket.sendMessage(sender, { text: downloadOptionsText }, { quoted: replyMek });
                        const optionsMsgID = downloadOptionsMsg.key.id;

                        const handleDownload = async ({ messages: downloadMessages }) => {
                            const downloadMek = downloadMessages[0];
                            if (!downloadMek?.message) return;

                            const downloadChoice = downloadMek.message.conversation || downloadMek.message.extendedTextMessage?.text;
                            const isReplyToOptionsMsg = downloadMek.message.extendedTextMessage?.contextInfo?.stanzaId === optionsMsgID;

                            if (isReplyToOptionsMsg && sender === downloadMek.key.remoteJid) {
                                const choiceNum = parseInt(downloadChoice) - 1;
                                
                                if (isNaN(choiceNum) || choiceNum < 0 || choiceNum >= validDownloads.length) {
                                    await socket.sendMessage(sender, {
                                        text: `*❪ INVALID ❫*\n\n⚠️ *Wrong Number!*\n🎯 *Range:* _01 - ${validDownloads.length}_\n📝 _Please reply with a valid number!_	ext ${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });
                                    return;
                                }

                                const selectedDownload = validDownloads[choiceNum];
                                await socket.sendMessage(sender, { react: { text: '📥', key: downloadMek.key } });

                                try {
                                    const finalDirectLink = selectedDownload.link;

                                    await socket.sendMessage(sender, {
                                        document: { url: finalDirectLink },
                                        mimetype: 'video/mp4',
                                        fileName: `${movieInfo.title} - ${selectedDownload.quality}.mp4`,
                                        caption: `*🎬 ÐΣVłŁ-X-MÐ 𝗖𝗜𝗡𝗘 𝗠𝗢𝗩𝗜𝗘 🎬*\n\n🎭 *Title:* ${movieInfo.title}\n🌟 *IMDB:* ${movieInfo.imdb || movieInfo.rating || 'N/A'}\n📅 *Year:* ${movieInfo.year || 'N/A'}\n📊 *Quality:* ${selectedDownload.quality}\n💾 *Size:* ${selectedDownload.size || 'N/A'}\n\n${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });

                                    await socket.sendMessage(sender, { react: { text: '✅', key: downloadMek.key } });

                                } catch (downloadError) {
                                    console.error('Download link error:', downloadError);
                                    await socket.sendMessage(sender, {
                                        text: `*❪ ERROR ❫*\n\n❌ *Download Failed!*\n🚫 _${downloadError.message}_${DEFAULT_FOOTER}`
                                    }, { quoted: downloadMek });
                                } finally {
                                    socket.ev.off('messages.upsert', handleDownload);
                                    socket.ev.off('messages.upsert', handleSelection);
                                }
                            }
                        };

                        socket.ev.on('messages.upsert', handleDownload);

                    } catch (detailsError) {
                        console.error('Details error:', detailsError);
                        await socket.sendMessage(sender, {
                            text: `*❪ ERROR ❫*\n\n❌ *Movie Details Error!*\n🚫 _${detailsError.message}_${DEFAULT_FOOTER}`
                        }, { quoted: replyMek });
                        socket.ev.off('messages.upsert', handleSelection);
                    }
                }
            }
        };

        socket.ev.on('messages.upsert', handleSelection);

    } catch (error) {
        console.error('Moviesublk command error:', error);
        await socket.sendMessage(sender, {
            text: `*❪ SYSTEM ERROR ❫*\n\n❌ *System Error!*\n🚫 _${error.message || 'Unknown error'}_\n\n🔄 _Please try again later..._${DEFAULT_FOOTER}`
        }, { quoted: msg });
    }
    
    break;
							}
              
              
              case 'report': {
          await socket.sendMessage(sender, { react: { text: '⚠️', key: msg.key } });
          try {
            const _rpSan = (number || '').replace(/[^0-9]/g, '');
            const _rpSenderNum = (nowsender || '').split('@')[0];
            const _rpOwnerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
            if (_rpSenderNum !== _rpSan && _rpSenderNum !== _rpOwnerNum) {
              return await socket.sendMessage(sender, { text: '❌ Only the session owner can use this command.' }, { quoted: msg });
            }

            // parse "number,count"
            const _rpRaw = (args[0] || '').trim();
            if (!_rpRaw.includes(',')) {
              return await socket.sendMessage(sender, {
                text: `📖 *Report Command Usage:*\n*.report number,count*\n\n*Example:*\n_.report 94789988778,10_\n\nThis will send 10 reports to that number.\n⚠️ Max 20 reports per command.`
              }, { quoted: msg });
            }

            const _rpParts = _rpRaw.split(',');
            const _rpTargetRaw = (_rpParts[0] || '').trim();
            const _rpCount = parseInt((_rpParts[1] || '').trim(), 10);

            if (!_rpTargetRaw || isNaN(_rpCount) || _rpCount < 1) {
              return await socket.sendMessage(sender, { text: '❗ Invalid format. Example: `.report 94789988778,10`' }, { quoted: msg });
            }

            const _rpMax = 20;
            const _rpFinal = Math.min(_rpCount, _rpMax);
            const _rpDigits = _rpTargetRaw.replace(/[^0-9]/g, '');
            const _rpJid = `${_rpDigits}@s.whatsapp.net`;

            if (!_rpDigits) {
              return await socket.sendMessage(sender, { text: '❗ Invalid phone number.' }, { quoted: msg });
            }

            await socket.sendMessage(sender, {
              text: `📡 *Sending ${_rpFinal} report(s) to* +${_rpDigits}...\n⏳ Please wait...`
            }, { quoted: msg });

            let _rpSuccess = 0;
            for (let _rpi = 0; _rpi < _rpFinal; _rpi++) {
              try {
                if (typeof socket.query === 'function') {
                  await socket.query({
                    tag: 'iq',
                    attrs: {
                      to: 's.whatsapp.net',
                      type: 'set',
                      xmlns: 'spam',
                      id: socket.generateMessageTag ? socket.generateMessageTag() : `report-${Date.now()}-${_rpi}`
                    },
                    content: [{
                      tag: 'report',
                      attrs: { v: '2', type: '1' },
                      content: [{
                        tag: 'user',
                        attrs: { jid: _rpJid }
                      }]
                    }]
                  });
                } else if (typeof socket.sendNode === 'function') {
                  await socket.sendNode({
                    tag: 'iq',
                    attrs: {
                      to: 's.whatsapp.net',
                      type: 'set',
                      xmlns: 'spam',
                      id: `report-${Date.now()}-${_rpi}`
                    },
                    content: [{
                      tag: 'report',
                      attrs: { v: '2', type: '1' },
                      content: [{
                        tag: 'user',
                        attrs: { jid: _rpJid }
                      }]
                    }]
                  });
                } else {
                  await socket.updateBlockStatus(_rpJid, 'block');
                  await delay(300);
                  await socket.updateBlockStatus(_rpJid, 'unblock');
                }
                _rpSuccess++;
              } catch(_rpErr) {
                console.log(`Report attempt ${_rpi + 1} error:`, _rpErr.message || _rpErr);
              }
              await delay(800);
            }

            await socket.sendMessage(sender, {
              text: `✅ *Report Complete!*\n\n📋 *Target:* +${_rpDigits}\n📊 *Reports Sent:* ${_rpSuccess}/${_rpFinal}\n${_rpSuccess < _rpFinal ? `⚠️ ${_rpFinal - _rpSuccess} failed (rate limit or invalid number)` : '🎯 All reports sent successfully!'}`
            }, { quoted: msg });
            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

          } catch(e) {
            console.error('report cmd error:', e);
            try { await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } }); } catch(re){}
            await socket.sendMessage(sender, { text: `❌ Report failed: ${e.message || e}` }, { quoted: msg });
          }
          break;
                            }
              case 'antilink': {
          if (!isGroup) return await socket.sendMessage(sender, { text: '❌ This command is for groups only.' }, { quoted: msg });
          await socket.sendMessage(sender, { react: { text: '🔗', key: msg.key } });
          try {
            let gAdmins = [];
            try { const m = await socket.groupMetadata(from); gAdmins = m.participants.filter(p => p.admin).map(p => p.id); } catch(e) {}
            if (!gAdmins.includes(nowsender) && !isOwner) return await socket.sendMessage(sender, { text: '❌ Only group admins can use this.' }, { quoted: msg });
            const opt = (args[0] || '').toLowerCase();
            if (opt === 'on' || opt === 'off') {
              await setGroupSetting(from, 'ANTI_LINK', opt);
              await socket.sendMessage(sender, { text: `✅ *Anti Link ${opt === 'on' ? 'ENABLED ✅' : 'DISABLED ❌'}*\nLinks will ${opt === 'on' ? 'now be deleted.' : 'no longer be deleted.'}` }, { quoted: msg });
            } else {
              await socket.sendMessage(sender, { text: `📖 *Anti Link:*\n.antilink on\n.antilink off` }, { quoted: msg });
            }
          } catch(e) { await socket.sendMessage(sender, { text: '❌ Error.' }, { quoted: msg }); }
          break;
        }

        case 'antispam': {
          if (!isGroup) return await socket.sendMessage(sender, { text: '❌ This command is for groups only.' }, { quoted: msg });
          await socket.sendMessage(sender, { react: { text: '🚫', key: msg.key } });
          try {
            let gAdmins = [];
            try { const m = await socket.groupMetadata(from); gAdmins = m.participants.filter(p => p.admin).map(p => p.id); } catch(e) {}
            if (!gAdmins.includes(nowsender) && !isOwner) return await socket.sendMessage(sender, { text: '❌ Only group admins can use this.' }, { quoted: msg });
            const opt = (args[0] || '').toLowerCase();
            if (opt === 'on' || opt === 'off') {
              await setGroupSetting(from, 'ANTI_SPAM', opt);
              await socket.sendMessage(sender, { text: `✅ *Anti Spam ${opt === 'on' ? 'ENABLED ✅' : 'DISABLED ❌'}*` }, { quoted: msg });
            } else {
              await socket.sendMessage(sender, { text: `📖 *Anti Spam:*\n.antispam on\n.antispam off` }, { quoted: msg });
            }
          } catch(e) { await socket.sendMessage(sender, { text: '❌ Error.' }, { quoted: msg }); }
          break;
        }
              
              
              case 'bomb': {
    const isOwner = senderNumber === config.OWNER_NUMBER;
    const isBotUser = activeSockets.has(senderNumber);

    if (!isOwner && !isBotUser) {
        return await socket.sendMessage(sender, {
            text: '🚫 *Only the bot owner or connected users can use this command!*'
        }, { quoted: msg });
    }

    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text || '';
    const [target, text, countRaw] = q.split(',').map(x => x?.trim());

    const count = parseInt(countRaw) || 5;

    if (!target || !text || !count) {
        return await socket.sendMessage(sender, {
            text: '📌 *Usage:* .bomb <number>,<message>,<count>\n\nExample:\n.bomb 9476XXXXXXX,Hello 👋,5'
        }, { quoted: msg });
    }

    const jid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;

    if (count > 20) {
        return await socket.sendMessage(sender, {
            text: '❌ *Limit is 20 messages per bomb.*'
        }, { quoted: msg });
    }

    for (let i = 0; i < count; i++) {
        await socket.sendMessage(jid, { text });
        await delay(700); // delay to prevent spam
    }

    await socket.sendMessage(sender, {
        text: `✅ Bomb sent to ${target} — ${count}x`
    }, { quoted: msg });

    break;
              }
              case 'contacts': {
          if (!isOwner) return;
          try {
            const data = await listGoogleContacts(sanitizedNum);
            if (!data.connections || data.connections.length === 0) {
              return await socket.sendMessage(from, { text: formatMessage('📔 GOOGLE CONTACTS', 'No contacts found.', botName) });
            }
            const list = data.connections.map(c => {
              const name = c.names?.[0]?.displayName || 'Unknown';
              const phone = c.phoneNumbers?.[0]?.value || 'No Number';
              return `👤 ${name}\n📞 ${phone}`;
            }).join('\n\n');
            await socket.sendMessage(from, { text: formatMessage('📔 GOOGLE CONTACTS', `*Top Contacts:*\n\n${list}`, botName) });
          } catch (e) {
            await socket.sendMessage(from, { text: formatMessage('❌ ERROR', e.message, botName) });
          }
          break;
              }
          
          
          case 'alone': {
  try {
    const desc = `
ABOUT ME – ALONE-X-MD V8 🇱🇰

Name: DAMITH MADUSANKA
Alias: ALONE-X-MD V8 🇱🇰
Age: 17+
Location: Auradhapura , Sri Lanka
Languages: Sinhala, English, Currently Learning Japanese
Profession: Creative Technologist, Bot Developer, Digital Designer, logo disaing
Team: DEV CODER TEAM
Dream Destinations: Japan & South Korea
Life Goal: Build a powerful future through tech and business — create Sri Lanka’s largest pawnshop network and the biggest vehicle yard, while giving my mother the life she deserves.

---

WHO I AM

I’m not just another face in the crowd — I’m ALONE-X-MD V8 🇱🇰, a self-made digital warrior. Born in the shadows of struggle, but trained in the light of purpose. I live not to follow trends, but to create legacies. I’ve made a vow: To rise, no matter how deep the fall.

---

WHAT I DO

Web Development:
I craft and code with HTML & JavaScript — from building websites to creating powerful panels and bot interfaces.

Bot Creator & DevOps:
I’m the mind behind ALONE-X-MD V8 🇱🇰 — a multi-functional WhatsApp bot featuring custom commands, automation, and system control. From .news to .apk, my bot does it all.

Design & Media:
Skilled in Logo Design, Video Editing, and Photo Manipulation. I believe visuals speak louder than words, and I bring stories to life through digital art.

Tech & AI Enthusiast:
I explore AI tools, automation systems, and even ethical hacking. I stay updated, learn fast, and adapt faster.

Purpose-Driven Learning:
Currently studying Japanese to prepare for my next journey — either to Japan or South Korea, where I plan to expand both my knowledge and my empire.

---

MY PHILOSOPHY

> “When the world turns dark, I don’t hide — I evolve. I am not afraid to walk alone in the shadows. I am the shadow. I am ALONE-X-MD V8 🇱🇰.”

====================••••••••==========

*මමත් ආසයි...🙂*

*හැමදේම කියන්න කෙනෙක් හිටියා නම්,*
*හැමවෙලේම මැසේජ් කරන්න,*
*කරදර කර කර හොයල බලන්න කෙනෙක් හිටියා නම්,*
*පරිස්සමෙන් ඉන්න මේ දවස් වල*
*මට ඉන්නෙ ඔයා විතරනෙ කියන්න කෙනෙක් හිටියා නම්,*
*මට දැනෙන තරම් මාව දැනෙන කෙනෙක් හිටියා නම්,*

*ඔව් ආදරේ කියන්නෙ*
*පරිස්සම් කරන එකට තමයි,*
*පරිස්සම් කරන්නෙ ආදරේ හින්දා තමයි,*

*ඉතින් ආදරේ කියන්නෙම පරිස්සම් කරන එකට තමයි...!❤‍🩹🥺*

*ස්තූතිය....!*

> ㋛︎ 𝐏ᴏᴡᴇʀᴅ 𝐁ʏ ALONE-X-MD V8 🇱🇰 
> ® ALONE-X-MD V8 🇱🇰 💧
`;

    const imageUrl = 'https://i.ibb.co/NdczhNhS/be4ab03a154e.jpg';

    await socket.sendMessage(sender, {
      image: { url: imageUrl },
      caption: desc
    }, { quoted: msg });

  } catch (e) {
    console.error("alone Command Error:", e);
    await socket.sendMessage(sender, { text: `❌ Error: ${e.message || e}` }, { quoted: msg });
  }
  break;
          }
          case 'menu':
case 'help':
case 'list': {
    try {
        await socket.sendMessage(sender, {
            react: { text: '🇱🇰', key: msg.key }
        });

        // ================= USER CONFIG =================
        const prefixUsed = sessionConfig?.PREFIX || config.PREFIX || '.';
        const userMention = `@${sender.split('@')[0]}`;

        const botName =
            sessionConfig?.BOT_NAME ||
            config.BOT_NAME ||
            'ÐΣVłŁ-X-MÐ';

        const footerText =
            sessionConfig?.BOT_FOOTER ||
            config.BOT_FOOTER ||
            '© ÐΣVłŁ-X-MÐ';

        // ================= UPTIME =================
        const startTime =
            socketCreationTime.get(number) || Date.now();

        const uptime = Math.floor(
            (Date.now() - startTime) / 1000
        );

        const days = Math.floor(uptime / 86400);
        const hours = Math.floor((uptime % 86400) / 3600);
        const minutes = Math.floor((uptime % 3600) / 60);
        const seconds = uptime % 60;

        const uptimeText =
            `${days}d ${hours}h ${minutes}m ${seconds}s`;

        // ================= MENU =================
        const caption =
`*╭╌╌╌╌◯*
*╎* \` 🐼 𝑯𝑬𝑳𝑳𝑶 𝑼𝑺𝑬𝑹 🐼ㅤㅤ\`
*╎🇱🇰⭓ BOT :* ${botName}
*╎🇱🇰⭓ TYPE :* ÐΣVłŁ-X-MÐ
*╎🇱🇰⭓ PLATFORM :* ʜᴇʀᴏᴋᴜ
*╎🇱🇰⭓ STATUS :* ᴏɴʟɪɴᴇ 💫
*╎🇱🇰⭓ USER :* ${userMention}
*╎🇱🇰⭓ PREFIX :* ${prefixUsed}
*╎🇱🇰⭓ UPTIME :* ${uptimeText}
*╰╌┬╌╌◯*
*╭╌┴╌╌◯*
*╎* \` 🐼 𝑩𝑶𝑻 𝑴𝑬𝑵𝑼 🐼ㅤㅤ\`
*╰━━━━━━━━━━━━━━━━━╯

┏━━━━━━━━━━━━━━━━━┓
┃ *🎬 MOVIE & TV MENU*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}cmovie <targetJid> <movie>
┃    └─ Multi-Site Channel Forwarder
┃
┃ 💗✦ ${prefixUsed}ridomovies <name>
┃    └─ Ridomovies Search
┃
┃ 💗✦ ${prefixUsed}movie <movie_name>
┃    └─ Multi-Site Movie Search
┃
┃ 💗✦ ${prefixUsed}cartoon <name>
┃    └─ Cartoon Search
┃
┃ 💗✦ ${prefixUsed}zoom <name>
┃    └─ Zoom.lk Search
┃
┃ 💗✦ ${prefixUsed}anime <name>
┃    └─ Anime Search
┃
┃ 💗✦ ${prefixUsed}cinesubz <name>
┃    └─ CineSubz Search
┃
┃ 💗✦ ${prefixUsed}moviesublk <name>
┃    └─ MovieSubLK Search
┃
┃ 💗✦ ${prefixUsed}sinhalasub <name>
┃    └─ SinhalaSub Search
┃
┃ 💗✦ ${prefixUsed}baiscope <name>
┃    └─ Baiscope LK Search
┃
┃ 💗✦ ${prefixUsed}thenkiri <name>
┃    └─ Thenkiri Search
┃
┃ 💗✦ ${prefixUsed}lak <name>
┃    └─ LakvisionTV Search
┃
┃ 💗✦ ${prefixUsed}moviebox <name>
┃    └─ MovieBox Search
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *🎵 DOWNLOAD MENU*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}song <name/url>
┃    └─ Download MP3
┃
┃ 💗✦ ${prefixUsed}csong <targetJid> <song>
┃    └─ Download & Forward Song
┃
┃ 💗✦ ${prefixUsed}asong <song>
┃    └─ Fast Song Download
┃
┃ 💗✦ ${prefixUsed}play <song>
┃    └─ YouTube Song
┃
┃ 💗✦ ${prefixUsed}fb <url>
┃    └─ Facebook Video
┃
┃ 💗✦ ${prefixUsed}tiktok <url>
┃    └─ TikTok Downloader
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *👥 GROUP MENU*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}kick @user
┃    └─ Remove Member
┃
┃ 💗✦ ${prefixUsed}add 94XXX
┃    └─ Add Member
┃
┃ 💗✦ ${prefixUsed}promote @user
┃    └─ Make Admin
┃
┃ 💗✦ ${prefixUsed}demote @user
┃    └─ Remove Admin
┃
┃ 💗✦ ${prefixUsed}mute
┃    └─ Close Group
┃
┃ 💗✦ ${prefixUsed}unmute
┃    └─ Open Group
┃
┃ 💗✦ ${prefixUsed}tagall <msg>
┃    └─ Tag All Members
┃
┃ 💗✦ ${prefixUsed}hidetag <msg>
┃    └─ Hidden Tag
┃
┃ 💗✦ ${prefixUsed}groupinfo
┃    └─ Group Information
┃
┃ 💗✦ ${prefixUsed}getdp
┃    └─ Get Group DP
┃
┃ 💗✦ ${prefixUsed}uinfo
┃    └─ User Information
┃
┃ 💗✦ ${prefixUsed}groupstatus
┃    └─ Group Status
┃
┃ 💗✦ ${prefixUsed}gstatus
┃    └─ Group Metadata
┃
┃ 💗✦ ${prefixUsed}setname
┃    └─ Change Group Name
┃
┃ 💗✦ ${prefixUsed}setdec
┃    └─ Change Group Description
┃
┃ 💗✦ ${prefixUsed}left <text>
┃    └─ Leave Group
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *🧰 FILE & MEDIA TOOLS*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}cfile <targetJid>
┃    └─ Forward Quoted Media
┃
┃ 💗✦ ${prefixUsed}forward
┃    └─ Forward Media
┃
┃ 💗✦ ${prefixUsed}fv <targetJid>
┃    └─ Forward Media
┃
┃ 💗✦ ${prefixUsed}rename <filename>
┃    └─ Rename Document
┃
┃ 💗✦ ${prefixUsed}tourl
┃    └─ Upload Media → URL
┃
┃ 💗✦ ${prefixUsed}imgbb
┃    └─ Upload Image → URL
┃
┃ 💗✦ ${prefixUsed}catbox
┃    └─ Upload Media → URL
┃
┃ 💗✦ ${prefixUsed}vv
┃    └─ View Once Unlock
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *📡 CHANNEL & SYSTEM*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}cid
┃    └─ Get Channel JID
┃
┃ 💗✦ ${prefixUsed}channelid <link>
┃    └─ Get Channel ID
┃
┃ 💗✦ ${prefixUsed}jid
┃    └─ Get Chat/Group JID
┃
┃ 💗✦ ${prefixUsed}ping
┃    └─ Check Bot Speed
┃
┃ 💗✦ ${prefixUsed}alive
┃    └─ Bot Alive Status
┃
┃ 💗✦ ${prefixUsed}system
┃    └─ CPU & Memory Info
┃
┃ 💗✦ ${prefixUsed}time
┃    └─ Country Time
┃
┃ 💗✦ ${prefixUsed}dev
┃    └─ Bot Information
┃
┃ 💗✦ ${prefixUsed}owner
┃    └─ Owner Contact
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *🤖 AI MENU*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}gf <message>
┃    └─ AI Chat
┃
┃ 💗✦ ${prefixUsed}bro <message>
┃    └─ AI Chat
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *✨ TEXT EFFECTS*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}3dcomic <text>
┃    └─ 3D Comic Style
┃
┃ 💗✦ ${prefixUsed}blackpink <text>
┃    └─ Pink Aesthetic
┃
┃ 💗✦ ${prefixUsed}neonlight <text>
┃    └─ Neon Glow
┃
┃ 💗✦ ${prefixUsed}naruto <text>
┃    └─ Anime Logo
┃
┃ 💗✦ ${prefixUsed}hacker <text>
┃    └─ Digital Hacker Style
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *📱 STATUS & REACTION*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}status <text/media>
┃    └─ Post WhatsApp Status
┃
┃ 💗✦ ${prefixUsed}autostatus <true/false>
┃    └─ Auto Status Viewer
┃
┃ 💗✦ ${prefixUsed}statusemoji <emoji>
┃    └─ Status Reaction Emoji
┃
┃ 💗✦ ${prefixUsed}addreact <channel_link>
┃    └─ Add Channel Reaction
┃
┃ 💗✦ ${prefixUsed}delreact <channel_link>
┃    └─ Remove Channel Reaction
┃
┃ 💗✦ ${prefixUsed}listreact
┃    └─ List Auto Reactions
┗━━━━━━━━━━━━━━━━━┛

┏━━━━━━━━━━━━━━━━━┓
┃ *👑 OWNER MENU*
┣━━━━━━━━━━━━━━━━━┫
┃ 💗✦ ${prefixUsed}setting <KEY:VALUE>
┃    └─ System Configuration
┃
┃ 💗✦ ${prefixUsed}alldp
┃    └─ Group Member DPs
┃
┃ 💗✦ ${prefixUsed}getabout
┃    └─ Get User About
┃
┃ 💗✦ ${prefixUsed}send
┃    └─ Send Saved Status
┃
┃ 💗✦ ${prefixUsed}vote <index>
┃    └─ Poll Vote
┗━━━━━━━━━━━━━━━━━┛

> ᴄᴏɴɴᴇᴄᴛ ʏᴏᴜʀ ɴᴜᴍʙᴇʀ
> ᴜꜱᴇ ${prefixUsed}pair <number>

> ᴠɪᴇᴡ-ᴏɴᴄᴇ ᴍᴇᴅɪᴀ
> ᴜꜱᴇ ${prefixUsed}vv

*𖹭 deploy .ᐟ _ᴏᴡɴᴇʀ/𝐌𝐚𝐃𝐮𝐒𝐚𝐍𝐤𝐀_*
╰──────────────────────────────╯`;

        // ================= SEND MENU =================
        await socket.sendMessage(sender, {
            image: {
                url: 'https://litter.catbox.moe/qb9z0z.jpg'
            },
            caption: caption,
            mentions: [sender],

            contextInfo: {
                forwardingScore: 1000,
                isForwarded: true,
                forwardedNewsletterMessageInfo: {
                    newsletterJid: '120363428670000697@newsletter',
                    newsletterName: 'ÐΣVłŁ-X-MÐ',
                    serverMessageId: 1
                }
            },

            buttons: [
                {
                    buttonId: `${prefixUsed}dev`,
                    buttonText: {
                        displayText: '💤 ʙᴏᴛ ɪɴꜰᴏ'
                    },
                    type: 1
                },
                {
                    buttonId: `${prefixUsed}alive`,
                    buttonText: {
                        displayText: '💫 ʙᴏᴛ ᴀʟɪᴠᴇ'
                    },
                    type: 1
                },
                {
                    buttonId: `${prefixUsed}owner`,
                    buttonText: {
                        displayText: '👑 ᴏᴡɴᴇʀ'
                    },
                    type: 1
                }
            ],

            headerType: 4
        }, { quoted: msg });

        await socket.sendMessage(sender, {
            react: { text: '✔️', key: msg.key }
        });

    } catch (error) {
        console.error('Menu Error:', error);

        await socket.sendMessage(sender, {
            text: `❌ Menu Error\n\n${error.message}`
        }, { quoted: msg });
    }

    break;
}
          case 'runtime': {
    try {
        const startTime = socketCreationTime.get(number) || Date.now();
        const uptime = Math.floor((Date.now() - startTime) / 1000);
        
        // Format time beautifully (e.g., "1h 5m 3s" or "5m 3s" if hours=0)
        const hours = Math.floor(uptime / 3600);
        const minutes = Math.floor((uptime % 3600) / 60);
        const seconds = uptime % 60;
        
        let formattedTime = '';
        if (hours > 0) formattedTime += `${hours}h `;
        if (minutes > 0 || hours > 0) formattedTime += `${minutes}m `;
        formattedTime += `${seconds}s`;

        // Get memory usage (optional)
        const memoryUsage = (process.memoryUsage().rss / (1024 * 1024)).toFixed(2) + " MB";

        await socket.sendMessage(sender, {
            image: { url: config.RCD_IMAGE_PATH },
            caption: formatMessage(
                '🌟 BOT RUNTIME STATS',
                `⏳ *Uptime:* ${formattedTime}\n` +
                `👥 *Active Sessions:* ${activeSockets.size}\n` +
                `📱 *Your Number:* ${number}\n` +
                `💾 *Memory Usage:* ${memoryUsage}\n\n` +
                `_𝐏ᴏᴡᴇʀᴅ 𝐁ʏ ÐΣVłŁ-X-MÐ_`,
                'ÐΣVłŁ-X-MÐ'
            ),
            contextInfo: { forwardingScore: 999, isForwarded: true }
        });
    } catch (error) {
        console.error("❌ Runtime command error:", error);
        await socket.sendMessage(sender, { 
            text: "⚠️ Failed to fetch runtime stats. Please try again later."
        });
    }
    break;
          }
          
          
          case 'img': {
          const q = body.replace(/^[.\/!]img\s*/i, '').trim();

          if (!q) return await socket.sendMessage(sender, {
            text: '🔍 Please provide a search query. Ex: .img sunset'
          }, { quoted: msg });

          try {
            const sanitized = (number || '').replace(/[^0-9]/g, '');
            const userCfg = await loadUserConfigFromMongo(sanitized) || {};
            const botName = userCfg.botName || BOT_NAME_FANCY;

            const botMention = {
              key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_IMG" },
              message: {
                contactMessage: {
                  displayName: botName,
                  vcard: `BEGIN:VCARD
VERSION:3.0
FN:${botName}
ORG:${botName}
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
              }
            };

            const res = await axios.get(`https://allstars-apis.vercel.app/pinterest?search=${encodeURIComponent(q)}`);
            const data = res.data?.data;

            if (!data || data.length === 0)
              return await socket.sendMessage(sender, { text: '❌ No images found.' }, { quoted: botMention });

            const randomImage = data[Math.floor(Math.random() * data.length)];

            await socket.sendMessage(sender, {
              image: { url: randomImage },
              caption: `🖼️ IMAGE SEARCH : ${q}\n\n> ${botName}`,
              buttons: [{
                buttonId: `${config.PREFIX}img ${q}`,
                buttonText: { displayText: "⏩ Next Image" },
                type: 1
              }],
              headerType: 4,
              contextInfo: { mentionedJid: [sender] }
            }, { quoted: botMention });

          } catch (err) {
            console.error("img error:", err);
            await socket.sendMessage(sender, { text: '❌ Failed to fetch images.' });
          }

          break;
          }
          case 'antidelete': {
          await socket.sendMessage(sender, { react: { text: '🗑️', key: msg.key } });
          try {
            const sanitized = (number || '').replace(/[^0-9]/g, '');
            const senderNum = (nowsender || '').split('@')[0];
            const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
            
            if (senderNum !== sanitized && senderNum !== ownerNum) {
              const shonux = {
                key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ANTIDELETE1" },
                message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
              };
              return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change anti delete setting.' }, { quoted: shonux });
            }
            
            let q = args[0];
            const settings = { on: "on", off: "off", group: "group", inbox: "inbox" };
            
            if (settings[q]) {
              const userConfig = await loadUserConfigFromMongo(sanitized) || {};
              userConfig.ANTI_DELETE = settings[q];
              await setUserConfigInMongo(sanitized, userConfig);
              
              let statusText = "";
              switch (q) {
                case "on":
                  statusText = "ENABLED FOR ALL CHATS";
                  break;
                case "off":
                  statusText = "DISABLED";
                  break;
                case "group":
                  statusText = "ENABLED FOR GROUPS ONLY";
                  break;
                case "inbox":
                  statusText = "ENABLED FOR INBOX ONLY";
                  break;
              }
              
              const shonux = {
                key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ANTIDELETE2" },
                message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
              };
              await socket.sendMessage(sender, { text: `✅ *Anti Delete: ${statusText}*` }, { quoted: shonux });
            } else {
              const shonux = {
                key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ANTIDELETE3" },
                message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
              };
              await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- on (all chats)\n- off (disabled)\n- group (groups only)\n- inbox (inbox only)" }, { quoted: shonux });
            }
          } catch (e) {
            console.error('Antidelete command error:', e);
            const shonux = {
                key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ANTIDELETE4" },
                message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
              };
            await socket.sendMessage(sender, { text: "*❌ Error updating your anti delete setting!*" }, { quoted: shonux });
          }
          break;
          }
          
          case 'about': {
    if (args.length < 1) {
        return await socket.sendMessage(sender, {
            text: "📛 *Usage:* `.about <number>`\n📌 *Example:* `.about 94787940686*`"
        });
    }

    const targetNumber = args[0].replace(/[^0-9]/g, '');
    const targetJid = `${targetNumber}@s.whatsapp.net`;

    // Reaction
    await socket.sendMessage(sender, {
        react: {
            text: "ℹ️",
            key: msg.key
        }
    });

    try {
        const statusData = await socket.fetchStatus(targetJid);
        const about = statusData.status || 'No status available';
        const setAt = statusData.setAt
            ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss')
            : 'Unknown';

        const timeAgo = statusData.setAt
            ? moment(statusData.setAt).fromNow()
            : 'Unknown';

        // Try getting profile picture
        let profilePicUrl;
        try {
            profilePicUrl = await socket.profilePictureUrl(targetJid, 'image');
        } catch {
            profilePicUrl = null;
        }

        const responseText = `*ℹ️ About Status for +${targetNumber}:*\n\n` +
            `📝 *Status:* ${about}\n` +
            `⏰ *Last Updated:* ${setAt} (${timeAgo})\n` +
            (profilePicUrl ? `🖼 *Profile Pic:* ${profilePicUrl}` : '');

        if (profilePicUrl) {
            await socket.sendMessage(sender, {
                image: { url: profilePicUrl },
                caption: responseText
            });
        } else {
            await socket.sendMessage(sender, { text: responseText });
        }
    } catch (error) {
        console.error(`Failed to fetch status for ${targetNumber}:`, error);
        await socket.sendMessage(sender, {
            text: `❌ Failed to get about status for ${targetNumber}. Make sure the number is valid and has WhatsApp.`
        });
    }
    break;
          }
          
          case 'group': {
          const sanitized = (number || '').replace(/[^0-9]/g, '');
          const cfg = await loadUserConfigFromMongo(sanitized) || {};
          const botName = cfg.botName || BOT_NAME_FANCY;
          const metaQ = { key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_GROUP_MENU" }, message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` } } };
          const gText = `*╭━━〔 👥 𝗚𝗥𝗢𝗨𝗣 𝗖𝗢𝗠𝗠𝗔𝗡𝗗𝗦 〕━━╮*\n*│*\n*│ 👤 Member Management*\n*│ .kick* @user — Remove member\n*│ .add* number — Add member\n*│ .promote* @user — Make admin\n*│ .demote* @user — Remove admin\n*│*\n*│ 🔒 Group Settings*\n*│ .mute* — Lock group (admins only)\n*│ .unmute* — Unlock group (everyone)\n*│ .groupname* name — Change group name\n*│ .groupdesc* desc — Change description\n*│ .grouplink* — Get invite link\n*│ .revoke* — Revoke invite link\n*│ .groupicon* — Set icon (reply to image)\n*│*\n*│ 🛡️ Group Protection*\n*│ .antilink on/off* — Block links in group\n*│ .antispam on/off* — Block spam messages\n*│ .welcome on/off* — Welcome new members\n*│ .goodbye on/off* — Goodbye messages\n*│*\n*│ 📢 Tag Commands*\n*│ .tagall* msg — Tag all members\n*│ .hidetag* msg — Silent tag all\n*│*\n*╰━━━━━━━━━━━━━━━━━╯*\n> *🍷 BOT CONNECTED 👉 cooming zoon*`;
          await socket.sendMessage(sender, { text: gText }, { quoted: metaQ });
          break;
          }
          case 'kick':
        case 'remove': {
          try {
            if (!from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });
            const gm = await socket.groupMetadata(from).catch(() => null);
            if (!gm) return await socket.sendMessage(sender, { text: '❌ Failed to get group info.' }, { quoted: msg });
            const me = (socket.user.id || '').split(':')[0] + '@s.whatsapp.net';
            const isAdmin = (gm.participants || []).find(p => (p.id || p.jid) === me && (p.admin === 'admin' || p.admin === 'superadmin'));
            if (!isAdmin) return await socket.sendMessage(sender, { text: '❌ Bot must be admin to kick members.' }, { quoted: msg });
            const ctx = msg.message?.extendedTextMessage?.contextInfo;
            let targetJid = ctx?.participant || (ctx?.mentionedJid && ctx.mentionedJid[0]);
            if (!targetJid && args[0]) { targetJid = args[0].replace(/[^0-9]/g, '') + '@s.whatsapp.net'; }
            if (!targetJid) return await socket.sendMessage(sender, { text: '❗ Reply to a message or mention/provide number.\n\nUsage: .kick @user' }, { quoted: msg });
            await socket.groupParticipantsUpdate(from, [targetJid], 'remove');
            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
            await socket.sendMessage(from, { text: `✅ @${targetJid.split('@')[0]} has been removed from the group.`, mentions: [targetJid] }, { quoted: msg });
          } catch (e) { console.error('kick error', e); await socket.sendMessage(sender, { text: '❌ Failed to kick: ' + (e.message || e) }, { quoted: msg }); }
          break;
        }

          case 'add': {
          try {
            if (!from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });
            if (!args[0]) return await socket.sendMessage(sender, { text: '❗ Usage: .add 94xxxxxxxxx' }, { quoted: msg });
            const gm = await socket.groupMetadata(from).catch(() => null);
            if (!gm) return await socket.sendMessage(sender, { text: '❌ Failed to get group info.' }, { quoted: msg });
            const me = (socket.user.id || '').split(':')[0] + '@s.whatsapp.net';
            const isAdmin = (gm.participants || []).find(p => (p.id || p.jid) === me && (p.admin === 'admin' || p.admin === 'superadmin'));
            if (!isAdmin) return await socket.sendMessage(sender, { text: '❌ Bot must be admin to add members.' }, { quoted: msg });
            const targetNum = args[0].replace(/[^0-9]/g, '');
            const targetJid = targetNum + '@s.whatsapp.net';
            await socket.groupParticipantsUpdate(from, [targetJid], 'add');
            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
            await socket.sendMessage(from, { text: `✅ @${targetNum} has been added to the group!`, mentions: [targetJid] }, { quoted: msg });
          } catch (e) { console.error('add error', e); await socket.sendMessage(sender, { text: '❌ Failed to add: ' + (e.message || e) }, { quoted: msg }); }
          break;
          }
          case 'demote': {
          try {
            if (!from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });
            const gm = await socket.groupMetadata(from).catch(() => null);
            if (!gm) return await socket.sendMessage(sender, { text: '❌ Failed to get group info.' }, { quoted: msg });
            const me = (socket.user.id || '').split(':')[0] + '@s.whatsapp.net';
            const isAdmin = (gm.participants || []).find(p => (p.id || p.jid) === me && (p.admin === 'admin' || p.admin === 'superadmin'));
            if (!isAdmin) return await socket.sendMessage(sender, { text: '❌ Bot must be admin to demote members.' }, { quoted: msg });
            const ctx = msg.message?.extendedTextMessage?.contextInfo;
            let targetJid = ctx?.participant || (ctx?.mentionedJid && ctx.mentionedJid[0]);
            if (!targetJid && args[0]) { targetJid = args[0].replace(/[^0-9]/g, '') + '@s.whatsapp.net'; }
            if (!targetJid) return await socket.sendMessage(sender, { text: '❗ Reply to a message or mention user.\n\nUsage: .demote @user' }, { quoted: msg });
            await socket.groupParticipantsUpdate(from, [targetJid], 'demote');
            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
            await socket.sendMessage(from, { text: `📉 @${targetJid.split('@')[0]} has been demoted from admin.`, mentions: [targetJid] }, { quoted: msg });
          } catch (e) { console.error('demote error', e); await socket.sendMessage(sender, { text: '❌ Failed to demote: ' + (e.message || e) }, { quoted: msg }); }
          break;
              }
          case 'promote': {
          try {
            if (!from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });
            const gm = await socket.groupMetadata(from).catch(() => null);
            if (!gm) return await socket.sendMessage(sender, { text: '❌ Failed to get group info.' }, { quoted: msg });
            const me = (socket.user.id || '').split(':')[0] + '@s.whatsapp.net';
            const isAdmin = (gm.participants || []).find(p => (p.id || p.jid) === me && (p.admin === 'admin' || p.admin === 'superadmin'));
            if (!isAdmin) return await socket.sendMessage(sender, { text: '❌ Bot must be admin to promote members.' }, { quoted: msg });
            const ctx = msg.message?.extendedTextMessage?.contextInfo;
            let targetJid = ctx?.participant || (ctx?.mentionedJid && ctx.mentionedJid[0]);
            if (!targetJid && args[0]) { targetJid = args[0].replace(/[^0-9]/g, '') + '@s.whatsapp.net'; }
            if (!targetJid) return await socket.sendMessage(sender, { text: '❗ Reply to a message or mention user.\n\nUsage: .promote @user' }, { quoted: msg });
            await socket.groupParticipantsUpdate(from, [targetJid], 'promote');
            await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
            await socket.sendMessage(from, { text: `⭐ @${targetJid.split('@')[0]} has been promoted to admin!`, mentions: [targetJid] }, { quoted: msg });
          } catch (e) { console.error('promote error', e); await socket.sendMessage(sender, { text: '❌ Failed to promote: ' + (e.message || e) }, { quoted: msg }); }
          break;
        }

          case 'unmute': {
          try {
            if (!from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });
            await socket.groupSettingUpdate(from, 'not_announcement');
            await socket.sendMessage(sender, { react: { text: '🔊', key: msg.key } });
            await socket.sendMessage(from, { text: '🔊 *Group has been unmuted!* Everyone can send messages now.' }, { quoted: msg });
          } catch (e) { await socket.sendMessage(sender, { text: '❌ Failed to unmute: ' + (e.message || e) }, { quoted: msg }); }
          break;
          }
          case 'mute': {
          try {
            if (!from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });
            await socket.groupSettingUpdate(from, 'announcement');
            await socket.sendMessage(sender, { react: { text: '🔇', key: msg.key } });
            await socket.sendMessage(from, { text: '🔇 *Group has been muted!* Only admins can send messages now.' }, { quoted: msg });
          } catch (e) { await socket.sendMessage(sender, { text: '❌ Failed to mute: ' + (e.message || e) }, { quoted: msg }); }
          break;
          }
          
          case 'boom': {
  try {
    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    await socket.sendMessage(sender, { react: { text: '💥', key: msg.key } });

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || '© ÐΣVłŁ-X-MÐ';

    // target is replied user or mentioned arg
    const targetNum = args[0] ? args[0].replace(/[^0-9]/g, '') : senderNumber;
    const targetJid = `${targetNum}@s.whatsapp.net`;

    // Animation frames — building up the explosion
    const frames = [
      '🌑 𝗟𝗼𝗮𝗱𝗶𝗻𝗴 𝗕𝗼𝗺𝗯...',
      '🌒 𝗔𝗿𝗺𝗶𝗻𝗴 𝗘𝘅𝗽𝗹𝗼𝘀𝗶𝘃𝗲...',
      '🌓 𝗙𝘂𝘀𝗲 𝗜𝗴𝗻𝗶𝘁𝗲𝗱... 🔥',
      '🌔 𝗖𝗼𝘂𝗻𝘁𝗱𝗼𝘄𝗻: 3️⃣...',
      '🌕 𝗖𝗼𝘂𝗻𝘁𝗱𝗼𝘄𝗻: 2️⃣...',
      '🌖 𝗖𝗼𝘂𝗻𝘁𝗱𝗼𝘄𝗻: 1️⃣...',
      '💥 *B O O M !*'
    ];

    const { key: animKey } = await socket.sendMessage(sender, { text: frames[0] }, { quoted: msg });

    for (let i = 1; i < frames.length; i++) {
      await sleep(700);
      await socket.sendMessage(sender, { text: frames[i], edit: animKey });
    }

    await sleep(600);

    // Final BOOM card
    const boomText = `
╭━━━━━━━━━━━━━━━━━━━━━╮
┃   💣 *B O O M !* 💣   ┃
╰━━━━━━━━━━━━━━━━━━━━━╯

💥💥💥💥💥💥💥💥💥💥💥
💥                                    💥
💥   @${targetNum} has been    💥
💥     B O M B E D ! 💣          💥
💥                                    💥
💥💥💥💥💥💥💥💥💥💥💥

🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥🔥
*𝗕𝗢𝗢𝗠𝗕𝗔𝗦𝗧𝗘𝗗 𝗕𝗬 ${botName}* 💥

> *© ᴘᴏᴡᴇʀᴇᴅ ʙʏ ${botName}*
`.trim();

    await socket.sendMessage(sender, {
      text: boomText,
      mentions: [targetJid]
    }, { quoted: msg });

    await socket.sendMessage(sender, { react: { text: '💣', key: msg.key } });

  } catch (e) {
    console.error('Boom command error:', e);
    await socket.sendMessage(sender, { text: '❌ Boom command failed.' }, { quoted: msg });
  }
  break;
          }
          case 'tourl':
        case 'url':
        case 'upload': {
          const axios = require('axios');
          const FormData = require('form-data');
          const fs = require('fs');
          const os = require('os');
          const path = require('path');

          const quoted = msg.message?.extendedTextMessage?.contextInfo;
          const mime = quoted?.quotedMessage?.imageMessage?.mimetype ||
            quoted?.quotedMessage?.videoMessage?.mimetype ||
            quoted?.quotedMessage?.audioMessage?.mimetype ||
            quoted?.quotedMessage?.documentMessage?.mimetype;

          if (!quoted || !mime) {
            return await socket.sendMessage(sender, { text: '❌ *Please reply to an image or video.*' });
          }

          // Fake Quote for Style
          const metaQuote = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_MEDIA" },
            message: { contactMessage: { displayName: "༺ ALONE X MD ꙰༻", vcard: `BEGIN:VCARD\nVERSION:3.0\nFN:Upload Service\nORG:Catbox/ImgBB\nEND:VCARD` } }
          };

          let mediaType;
          let msgKey;

          if (quoted.quotedMessage.imageMessage) {
            mediaType = 'image';
            msgKey = quoted.quotedMessage.imageMessage;
          } else if (quoted.quotedMessage.videoMessage) {
            mediaType = 'video';
            msgKey = quoted.quotedMessage.videoMessage;
          } else if (quoted.quotedMessage.audioMessage) {
            mediaType = 'audio';
            msgKey = quoted.quotedMessage.audioMessage;
          } else if (quoted.quotedMessage.documentMessage) {
            mediaType = 'document';
            msgKey = quoted.quotedMessage.documentMessage;
          }

          try {
            // Using existing downloadContentFromMessage
            const stream = await downloadContentFromMessage(msgKey, mediaType);
            let buffer = Buffer.alloc(0);
            for await (const chunk of stream) {
              buffer = Buffer.concat([buffer, chunk]);
            }

            const ext = mime.split('/')[1] || 'tmp';
            const tempFilePath = path.join(os.tmpdir(), `upload_${Date.now()}.${ext}`);
            fs.writeFileSync(tempFilePath, buffer);

            const fileSize = (buffer.length / 1024 / 1024).toFixed(2) + ' MB';
            const typeStr = mediaType.charAt(0).toUpperCase() + mediaType.slice(1);

            let catboxUrl = '';
            let imgbbUrl = '';

            // Upload to Catbox
            try {
              const catboxForm = new FormData();
              catboxForm.append('fileToUpload', fs.createReadStream(tempFilePath));
              catboxForm.append('reqtype', 'fileupload');

              const catboxResponse = await axios.post('https://catbox.moe/user/api.php', catboxForm, {
                headers: catboxForm.getHeaders()
              });
              catboxUrl = catboxResponse.data.trim();
            } catch (catboxError) {
              console.error('Catbox upload error:', catboxError);
              catboxUrl = '❌ Upload failed';
            }

            // Upload to ImgBB (works best with images)
            try {
              const base64Data = buffer.toString('base64');
              const imgbbForm = new FormData();
              imgbbForm.append('key', 'e4b536bbf102cfccc5d8758489052547');
              imgbbForm.append('image', base64Data);

              const imgbbResponse = await axios.post('https://api.imgbb.com/1/upload', imgbbForm, {
                headers: imgbbForm.getHeaders()
              });

              if (imgbbResponse.data.success) {
                imgbbUrl = imgbbResponse.data.data.url;
              } else {
                imgbbUrl = '❌ Upload failed';
              }
            } catch (imgbbError) {
              console.error('ImgBB upload error:', imgbbError);
              imgbbUrl = '❌ Upload failed';
            }

            // Cleanup
            fs.unlinkSync(tempFilePath);

            // Prepare message
            const txt = `
🔗 *ÐΣVłŁ-X-MÐ 𝗨ʀʟ 𝗖ᴏɴᴠᴇɴᴛᴇʀ*

📂 *ᴛʏᴘᴇ:* ${typeStr}
📊 *ꜱɪᴢᴇ:* ${fileSize}

📦 *ᴄᴀᴛʙᴏx ᴜʀʟ:*
${catboxUrl}

📦 *ɪᴍɢʙʙ ᴜʀʟ:*
${imgbbUrl}

> *𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝐁𝐘 ÐΣVłŁ-X-MÐ*`;

            // Determine thumbnail for preview
            let thumbnailUrl = "https://cdn-icons-png.flaticon.com/512/337/337946.png";
            if (catboxUrl && !catboxUrl.includes('❌') && catboxUrl.match(/\.(jpeg|jpg|gif|png)$/i)) {
              thumbnailUrl = catboxUrl;
            } else if (imgbbUrl && !imgbbUrl.includes('❌')) {
              thumbnailUrl = imgbbUrl;
            }

            await socket.sendMessage(sender, {
              text: txt,
              contextInfo: {
                externalAdReply: {
                  title: "Media Uploaded Successfully!",
                  body: "Dual Upload Service",
                  thumbnailUrl: thumbnailUrl,
                  sourceUrl: catboxUrl && !catboxUrl.includes('❌') ? catboxUrl : (imgbbUrl && !imgbbUrl.includes('❌') ? imgbbUrl : ''),
                  mediaType: 1,
                  renderLargerThumbnail: true
                }
              }
            }, { quoted: metaQuote });

          } catch (e) {
            console.error(e);
            await socket.sendMessage(sender, { text: '❌ *Error uploading media.*' });
          }
        }
          break;
          
          case 'system': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;
    const logo = cfg.logo || config.RCD_IMAGE_PATH;

    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SYSTEM" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    const os = require('os');
    const text = `
🖥️ *System Info for ${botName}*
💻 OS: ${os.type()} ${os.release()}
🖥️ Platform: ${os.platform()}
🧠 CPU cores: ${os.cpus().length}
💾 Memory: ${(os.totalmem()/1024/1024/1024).toFixed(2)} GB
`;

    let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

    await socket.sendMessage(sender, {
      image: imagePayload,
      caption: text,
      footer: `🔥 ${botName} SYSTEM INFO 🔥`,
      buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }],
      headerType: 4
    }, { quoted: metaQuote });

  } catch(e) {
    console.error('system error', e);
    await socket.sendMessage(sender, { text: '❌ Failed to get system info.' }, { quoted: msg });
  }
  break;
          }
          
          case 'jid': {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || '© ÐΣVłŁ-X-MÐ'; // dynamic bot name

    const userNumber = sender.split('@')[0]; 

    // Reaction
    await socket.sendMessage(sender, { 
        react: { text: "🆔", key: msg.key } 
    });

    // Fake contact quoting for meta style
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_FAKE_ID" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, {
        text: `*🆔 𝐂hat 𝐉ID:* ${sender}\n*📞 𝐘our 𝐍umber:* +${userNumber}`,
    }, { quoted: shonux });
    break;
          }
          
        
          
          
          case 'pair': {
           
    // ✅ Fix for node-fetch v3.x (ESM-only module)
    const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';

    // අංකය ලබා ගැනීම (Remove command text)
    const number = q.replace(/^[.\/!]pair\s*/i, '').trim();

    if (!number) {
        return await socket.sendMessage(sender, {
            text: '*📌 Usage:* .pair 947XXXXXXX'
        }, { quoted: msg });
    }

    try {
        // ✅ NEW API URL UPDATED
        const url = `https://alone-x-md-production.up.railway.app/code?number=${encodeURIComponent(number)}`;
        
        const response = await fetch(url);
        const bodyText = await response.text();

        // console.log("🌐 API Response:", bodyText); // Debugging purpose

        let result;
        try {
            result = JSON.parse(bodyText);
        } catch (e) {
            console.error("❌ JSON Parse Error:", e);
            return await socket.sendMessage(sender, {
                text: '❌ Invalid response from server. Please contact support.'
            }, { quoted: msg });
        }

        if (!result || !result.code) {
            return await socket.sendMessage(sender, {
                text: `❌ Failed to retrieve pairing code.\nReason: ${result?.message || 'Check the number format'}`
            }, { quoted: msg });
        }

        // React sending
        await socket.sendMessage(sender, { react: { text: '🔑', key: msg.key } });

        // Send Main Message
        await socket.sendMessage(sender, {
            text: `> *ᴄᴏᴅᴇ ɪꜱ  ᴄᴏᴍᴘʟᴇᴀᴛᴇ* ✅\n\n*🔑 ʏᴏᴜ ᴄᴀɴᴛ ᴘᴀɪʀ ᴛʜɪꜱ ʙᴏᴛ.\n ᴛʜɪꜱ ʙᴏᴛ ɪꜱ ᴏɴʟʏ ᴛᴇꜱᴛᴇʀ* ${result.code}\n
`
        }, { quoted: msg });

        await sleep(2000);

        // Send Code Separately for easy copy
        await socket.sendMessage(sender, {
            text: `${result.code}`
        }, { quoted: msg });

    } catch (err) {
        console.error("❌ Pair Command Error:", err);
        await socket.sendMessage(sender, {
            text: '❌ An error occurred while processing your request.'
        }, { quoted: msg });
    }

    break;
                                 }
          case 'getdp': {
          try {
            const sanitized = (number || '').replace(/[^0-9]/g, '');
            const cfg = await loadUserConfigFromMongo(sanitized) || {};

            const botName = cfg.botName || "༺ ALONE X MD ꙰༻";
            const logo = cfg.logo || config.RCD_IMAGE_PATH;

            // ✅ get number from message
            let q = msg.message?.conversation?.split(" ")[1] ||
              msg.message?.extendedTextMessage?.text?.split(" ")[1];

            if (!q) {
              return await socket.sendMessage(sender, {
                text: `❌ Please provide a number!\n\nUsage: ${config.PREFIX}getdp 947XXXXXXXX`
              });
            }

            // ✅ format JID
            let jid = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

            // ✅ get profile picture
            let ppUrl;
            try {
              ppUrl = await socket.profilePictureUrl(jid, "image");
            } catch {
              ppUrl = "https://files.catbox.moe/uqjp2b.jpeg"; // default fallback
            }

            // ✅ meta quote (clean version)
            const metaQuote = {
              key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "GETDP_META"
              },
              message: {
                contactMessage: {
                  displayName: botName,
                  vcard: `BEGIN:VCARD
VERSION:3.0
FN:${botName}
ORG:${botName}
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
              }
            };

            // ✅ send DP
            await socket.sendMessage(sender, {
              image: { url: ppUrl },
              caption: `
╭━━〔 🖼️ *PROFILE PICTURE* 〕━━⬣
┃ 📱 Number : +${q}
┃ 🤖 Bot : ${botName}
╰━━━━━━━━━━━━━━━━━━⬣
> ⚡ Fast DP Fetcher
      `.trim(),
              footer: `🇱🇰 ${botName}`,
              buttons: [
                {
                  buttonId: `${config.PREFIX}menu`,
                  buttonText: { displayText: "📑 Menu" },
                  type: 1
                }
              ],
              headerType: 4
            }, { quoted: metaQuote });

          } catch (e) {
            console.log("❌ getdp error:", e);

            await socket.sendMessage(sender, {
              text: "⚠️ Error: Could not fetch profile picture."
            });
          }

          break;
          }
          



          case 'hack': {
  try {
    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    await socket.sendMessage(sender, { react: { text: '💻', key: msg.key } });

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || '© ༺ ALONE X MD ꙰༻ ||🍃';

    const targetNum = args[0] ? args[0].replace(/[^0-9]/g, '') : senderNumber;
    const targetJid = `${targetNum}@s.whatsapp.net`;

    // Fake hacking animation frames
    const hackFrames = [
      '```[●] Initializing hack sequence...```',
      '```[●] Connecting to target: +' + targetNum + '...```',
      '```[●] Bypassing firewall... ██░░░░░░ 25%```',
      '```[●] Cracking encryption... ████░░░░ 50%```',
      '```[●] Accessing database... ██████░░ 75%```',
      '```[●] Extracting data...    ████████ 99%```',
      '```[✔] ACCESS GRANTED 🔓```'
    ];

    const { key: hackKey } = await socket.sendMessage(sender, { text: hackFrames[0] }, { quoted: msg });

    for (let i = 1; i < hackFrames.length; i++) {
      await sleep(900);
      await socket.sendMessage(sender, { text: hackFrames[i], edit: hackKey });
    }

    await sleep(700);

    // Final hack result card
    const hackResult = `
╭━━━━━━━━━━━━━━━━━━━━╮
┃  💻 *𝙷 𝙰 𝙲 𝙺 𝙴 𝙳 !* 🔓  ┃
╰━━━━━━━━━━━━━━━━━━━━╯

🖥️ *𝚃𝙰𝚁𝙶𝙴𝚃:* @${targetNum}
📡 *𝚂𝚃𝙰𝚃𝚄𝚂:* 🟡 𝗖𝗼𝗺𝗽𝗿𝗼𝗺𝗶𝘀𝗲𝗱

┌─────────────────────
│ 📁 𝗙𝗶𝗹𝗲𝘀 𝗔𝗰𝗰𝗲𝘀𝘀𝗲𝗱   : 9,999
│ 🔑 𝗣𝗮𝘀𝘀𝘄𝗼𝗿𝗱𝘀 𝗙𝗼𝘂𝗻𝗱  : 1234
│ 📍 𝗟𝗼𝗰𝗮𝘁𝗶𝗼𝗻 𝗧𝗿𝗮𝗰𝗸𝗲𝗱 : 🌐 Online
│ 📷 𝗖𝗮𝗺𝗲𝗿𝗮 𝗛𝗮𝗰𝗸𝗲𝗱   : ✅ Active
│ 📞 𝗖𝗮𝗹𝗹𝘀 𝗥𝗲𝗰𝗼𝗿𝗱𝗲𝗱  : ✅ Logging
└─────────────────────

⚠️ _This is just for fun — no real hacking!_ ⚠️

> *© ᴘᴏᴡᴇʀᴇᴅ ʙʏ ${botName}*
`.trim();

    await socket.sendMessage(sender, {
      text: hackResult,
      mentions: [targetJid]
    }, { quoted: msg });

    await socket.sendMessage(sender, { react: { text: '🔓', key: msg.key } });

  } catch (e) {
    console.error('Hack command error:', e);
    await socket.sendMessage(sender, { text: '❌ Hack command failed.' }, { quoted: msg });
  }
  break;
          }
          
case 'දාපන්': case 'oni': case 'vv': case 'save': {
          try {
            const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
            if (!quotedMsg) return await socket.sendMessage(sender, { text: '*❌ Please reply to a message (status/media) to save it.*' }, { quoted: msg });
            try { await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } }); } catch (e) { }
            const saveChat = sender;
            if (quotedMsg.imageMessage || quotedMsg.videoMessage || quotedMsg.audioMessage || quotedMsg.documentMessage || quotedMsg.stickerMessage) {
              const media = await downloadQuotedMedia(quotedMsg);
              if (!media || !media.buffer) return await socket.sendMessage(sender, { text: '❌ Failed to download media.' }, { quoted: msg });
              if (quotedMsg.imageMessage) await socket.sendMessage(saveChat, { image: media.buffer, caption: media.caption || '✅ Status Saved' });
              else if (quotedMsg.videoMessage) await socket.sendMessage(saveChat, { video: media.buffer, caption: media.caption || '✅ Status Saved', mimetype: media.mime || 'video/mp4' });
              else if (quotedMsg.audioMessage) await socket.sendMessage(saveChat, { audio: media.buffer, mimetype: media.mime || 'audio/mp4', ptt: media.ptt || false });
              else if (quotedMsg.documentMessage) { const fname = media.fileName || `saved_document.${(await FileType.fromBuffer(media.buffer))?.ext || 'bin'}`; await socket.sendMessage(saveChat, { document: media.buffer, fileName: fname, mimetype: media.mime || 'application/octet-stream' }); }
              else if (quotedMsg.stickerMessage) await socket.sendMessage(saveChat, { image: media.buffer, caption: media.caption || '✅ Sticker Saved' });
              await socket.sendMessage(sender, { text: '🔥 *𝐒tatus 𝐒aved 𝐒uccessfully!*' }, { quoted: msg });
            } else if (quotedMsg.conversation || quotedMsg.extendedTextMessage) {
              const text = quotedMsg.conversation || quotedMsg.extendedTextMessage.text;
              await socket.sendMessage(saveChat, { text: `✅ *𝐒tatus 𝐒aved*\n\n${text}` });
              await socket.sendMessage(sender, { text: '🔥 *𝐓ext 𝐒tatus 𝐒aved 𝐒uccessfully!*' }, { quoted: msg });
            } else { await socket.sendMessage(sender, { text: '❌ Unsupported quoted message type.' }, { quoted: msg }); }
          } catch (error) { console.error('❌ Save error:', error); await socket.sendMessage(sender, { text: '*❌ Failed to save status*' }, { quoted: msg }); }
          break;
        }


case 'alive': {
          try {
            const sanitized = (number || '').replace(/[^0-9]/g, '');
            const cfg = await loadUserConfigFromMongo(sanitized) || {};
            const botName = cfg.botName || BOT_NAME_FANCY;
            const logo = cfg.logo || config.RCD_IMAGE_PATH;
            const now = new Date();
            const sriLankaTime = now.toLocaleString('en-US', { timeZone: 'Asia/Colombo' });
            const sriLankaDate = new Date(sriLankaTime);
            const currentHour = sriLankaDate.getHours();
            let greeting = currentHour >= 5 && currentHour < 12 ? 'Good Morning 🌅' : (currentHour >= 12 && currentHour < 18 ? 'Good Afternoon' : 'Good Evening 🌙');
            const formattedDate = sriLankaDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric', timeZone: 'Asia/Colombo' });
            const formattedDay = sriLankaDate.toLocaleDateString('en-US', { weekday: 'long', timeZone: 'Asia/Colombo' });
            const formattedTime = sriLankaDate.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true, timeZone: 'Asia/Colombo' });
            const startTime = socketCreationTime.get(number) || Date.now();
            const uptime = Math.floor((Date.now() - startTime) / 1000);
            const hours = Math.floor(uptime / 3600);
            const minutes = Math.floor((uptime % 3600) / 60);
            const seconds = Math.floor(uptime % 60);
            const text = `*𝗛ɪ 👋 ${botName}*\n\n*╭───────────╮*\n*┃🗯️ 𝗚ʀᴇᴇᴛɪɴɢ :* ${greeting}\n*┃🗓️ 𝗗ᴀᴛᴇ  :* ${formattedDate}\n*┃📆 𝗗ᴀʏ  :* ${formattedDay}\n*┃⏱️ 𝗧ɪᴍᴇ :* ${formattedTime} (IST)\n*┃📄 𝗕ᴏᴛ 𝗡ᴀᴍᴇ :* ${botName}\n*┃🥷 𝗢ᴡɴᴇʀ :* ${config.OWNER_NAME || '@ÐΣVłŁ-X-MÐ'}\n*┃🧬 𝗩ᴇʀꜱɪᴏɴ :* 8.0.0\n*┃🎈 𝗣ʟᴀᴛꜰᴏʀᴍ :* ${process.env.PLATFORM || '𝗛eroku'}\n*┃📟 𝗨ᴘᴛɪᴍᴇ :* ${hours}h ${minutes}m ${seconds}s\n*┃✒️ 𝗣ʀᴇꜰɪx :* .\n*╰────────────╯*`;
            let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);
            await socket.sendMessage(sender, { image: imagePayload, caption: text });
          } catch (e) { console.error('alive error', e); await socket.sendMessage(sender, { text: '❌ Failed to send alive status.' }, { quoted: msg }); }
          break;
        }

        // ==================== PING COMMAND ====================
        case 'ping': {
          try {
            const start = Date.now();
            const sanitized = (number || '').replace(/[^0-9]/g, '');
            const cfg = await loadUserConfigFromMongo(sanitized) || {};
            const botName = cfg.botName || BOT_NAME_FANCY;
            const logo = cfg.logo || config.RCD_IMAGE_PATH;
            const userTag = `@${sender.split("@")[0]}`;
            const now = new Date();
            const sriLankaTime = now.toLocaleString('en-US', { timeZone: 'Asia/Colombo' });
            const sriLankaDate = new Date(sriLankaTime);
            const currentHour = sriLankaDate.getHours();
            let greeting = currentHour >= 5 && currentHour < 12 ? 'Good Morning 🌅' : (currentHour >= 12 && currentHour < 18 ? 'Good Afternoon ☀️' : 'Good Evening 🌙');
            const formattedTime = sriLankaDate.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true, timeZone: 'Asia/Colombo' });
            const uptime = process.uptime();
            const hours = Math.floor(uptime / 3600);
            const minutes = Math.floor((uptime % 3600) / 60);
            const seconds = Math.floor(uptime % 60);
            const end = Date.now();
            const latency = end - start;
            const speedStatus = latency < 200 ? 'Excellent 🟢' : latency < 500 ? 'Good 🟡' : 'Slow 🔴';
            const text = `🏓 🇱🇰 𝗣𝗢𝗡𝗚 𝗥𝗘𝗦𝗨𝗟𝗧\n\n👤 USER : ${userTag}\n🗯️ GREETING : ${greeting}\n⏰ TIME : ${formattedTime}\n\n⚡ SPEED : ${latency} ms\n🖥️ RUNTIME : ${hours}h ${minutes}m ${seconds}s\n📡 STATUS : ${speedStatus}\n\nThanks for using ${botName} 🚀`;
            let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);
            await socket.sendMessage(sender, { image: imagePayload, caption: text });
          } catch (e) { console.error('ping error', e); await socket.sendMessage(sender, { text: '❌ Failed to test ping.' }, { quoted: msg }); }
          break;
        }

        // ==================== OWNER COMMAND ====================
        case 'owner': {
  try {

    // ================= REACTION =================
    await socket.sendMessage(sender, {
      react: {
        text: "🥷",
        key: msg.key
      }
    });

    // ================= OWNER CONFIG =================
    const BOT_NAME = "ÐΣVłŁ-X-MÐ";
    const OWNER_NAME = "DINIDU";
    const OWNER_NUMBER = "94729458311";
    const DISPLAY_NUMBER = "+94 72 945 8311";
    const EMAIL = "dinidu@email.com";

    // ================= VCARD =================
    const vcard = [
      "BEGIN:VCARD",
      "VERSION:3.0",
      `FN:${OWNER_NAME}`,
      `ORG:${BOT_NAME}`,
      `TEL;type=CELL;type=VOICE;waid=${OWNER_NUMBER}:${DISPLAY_NUMBER}`,
      `EMAIL:${EMAIL}`,
      "END:VCARD"
    ].join("\n");

    // ================= SEND CONTACT =================
    await socket.sendMessage(sender, {
      contacts: {
        displayName: OWNER_NAME,
        contacts: [{ vcard }]
      }
    });

    // ================= OWNER CARD =================
    const ownerCard = `
╭━━━〔 👑 𝐎𝐖𝐍𝐄𝐑 〕━━━╮
┃
┃  🤖 𝐁𝐎𝐓
┃  ${BOT_NAME}
┃
┃  👤 𝐍𝐀𝐌𝐄
┃  ${OWNER_NAME}
┃
┃  📱 𝐖𝐇𝐀𝐓𝐒𝐀𝐏𝐏
┃  ${DISPLAY_NUMBER}
┃
┃  📧 𝐄𝐌𝐀𝐈𝐋
┃  ${EMAIL}
┃
╰━━━━━━━━━━━━━━━━━━╯

╭━━〔 ⚡ 𝐎𝐖𝐍𝐄𝐑 𝐈𝐍𝐅𝐎 〕━━╮
┃
┃  🥷 𝑩𝒐𝒕 𝑪𝒓𝒆𝒂𝒕𝒐𝒓
┃  👑 𝑺𝒚𝒔𝒕𝒆𝒎 𝑶𝒘𝒏𝒆𝒓
┃  ⚡ 𝑩𝒐𝒕 𝑫𝒆𝒗𝒆𝒍𝒐𝒑𝒆𝒓
┃
╰━━━━━━━━━━━━━━━━━━╯

> 🖤 Powered by ${BOT_NAME}
> 🇱🇰 Sri Lanka • WhatsApp Bot
`.trim();

    await socket.sendMessage(
      sender,
      { text: ownerCard },
      { quoted: msg }
    );

  } catch (e) {

    console.error("owner command error:", e);

    await socket.sendMessage(
      sender,
      {
        text: "❌ *OWNER INFO ERROR*\n\nUnable to send owner details."
      },
      { quoted: msg }
    );
  }

  break;
}

        // ==================== AUTO TYPING ====================
        case 'autotyping': {
  try {

    // ================= USER CONFIG =================
    const cfg = await loadUserConfigFromMongo(sanitized) || {
      ...config.DEFAULT_SETTINGS
    };

    // ================= TOGGLE AUTO TYPING =================
    cfg.AUTO_TYPING = cfg.AUTO_TYPING === 'true'
      ? 'false'
      : 'true';

    // ================= SAVE USER CONFIG =================
    await setUserConfigInMongo(sanitized, cfg);

    // ================= RESPONSE =================
    if (cfg.AUTO_TYPING === 'true') {

      await socket.sendMessage(
        sender,
        {
          text:
            '*AUTO TYPING* 🟢 *ENABLED*\n\n' +
            '🤖 Bot will show typing indicator.'
        },
        { quoted: msg }
      );

    } else {

      await socket.sendMessage(
        sender,
        {
          text:
            '*AUTO TYPING* 🔴 *DISABLED*\n\n' +
            '🤖 Typing indicator disabled.'
        },
        { quoted: msg }
      );

    }

  } catch (e) {

    console.error('autotyping error:', e);

    await socket.sendMessage(
      sender,
      {
        text: '❌ Error updating auto typing.'
      },
      { quoted: msg }
    );

  }

  break;
}
        // ==================== AUTO VOICE ====================
        case 'autovoice': {
  try {

    // 👤 User-specific config
    const cfg =
      await loadUserConfigFromMongo(sanitized) || {
        ...config.DEFAULT_SETTINGS
      };

    // 🔄 Toggle ON / OFF
    cfg.AUTO_VOICE = cfg.AUTO_VOICE === 'on'
      ? 'off'
      : 'on';

    await setUserConfigInMongo(sanitized, cfg);

    const isOn = cfg.AUTO_VOICE === 'on';
    const status = isOn ? '✅ ENABLED' : '❌ DISABLED';

    const voiceText = `
╔══════════════════════════╗
  🎙️  𝗔𝗨𝗧𝗢 𝗩𝗢𝗜𝗖𝗘 ${status}
╚══════════════════════════╝

${isOn
  ? '  🔊 𝗔𝘂𝘁𝗼 𝘃𝗼𝗶𝗰𝗲 𝗶𝘀 𝗻𝗼𝘄 𝗮𝗰𝘁𝗶𝘃𝗲!\n  🎵 𝗩𝗼𝗶𝗰𝗲 𝗿𝗲𝘀𝗽𝗼𝗻𝘀𝗲𝘀 𝘄𝗶𝗹𝗹 𝗯𝗲 𝘀𝗲𝗻𝘁\n  𝗳𝗼𝗿: 𝗵𝗶, 𝗵𝗲𝗹𝗹𝗼, 𝗴𝗺, 𝗴𝗻, 𝗯𝘆𝗲...'
  : '  🔇 𝗔𝘂𝘁𝗼 𝘃𝗼𝗶𝗰𝗲 𝗶𝘀 𝗻𝗼𝘄 𝗱𝗶𝘀𝗮𝗯𝗹𝗲𝗱.\n  📵 𝗡𝗼 𝘃𝗼𝗶𝗰𝗲 𝗺𝗲𝘀𝘀𝗮𝗴𝗲𝘀 𝘄𝗶𝗹𝗹 𝗯𝗲 𝘀𝗲𝗻𝘁.'}

> *© ÐΣVłŁ-X-MÐ*`;

    await socket.sendMessage(
      sender,
      { text: voiceText },
      { quoted: msg }
    );

  } catch (e) {

    console.error('autovoice error:', e);

    await socket.sendMessage(
      sender,
      {
        text: '❌ Error updating auto voice.'
      },
      { quoted: msg }
    );
  }

  break;
}
        // ==================== AUTO RECORDING ====================
        case 'autorecording': {
          try {
            const cfg = await loadUserConfigFromMongo(sanitized) || config.DEFAULT_SETTINGS;
            cfg.AUTO_RECORDING = cfg.AUTO_RECORDING === 'true' ? 'false' : 'true';
            await setUserConfigInMongo(sanitized, cfg);
            const status = cfg.AUTO_RECORDING === 'true' ? '✅ ENABLED' : '❌ DISABLED';
            await socket.sendMessage(sender, { text: `*AUTO RECORDING* ${status}\n\n${cfg.AUTO_RECORDING === 'true' ? '🎙️ Recording indicator activated' : '⏹️ Recording indicator disabled'}` }, { quoted: msg });
          } catch (e) { console.error('autorecording error:', e); await socket.sendMessage(sender, { text: '❌ Error updating auto recording.' }, { quoted: msg }); }
          break;
        }

        // ==================== READ STATUS ====================
        case 'rstatus': {
          try {
            const cfg = await loadUserConfigFromMongo(sanitized) || config.DEFAULT_SETTINGS;
            cfg.AUTO_VIEW_STATUS = cfg.AUTO_VIEW_STATUS === 'true' ? 'false' : 'true';
            await setUserConfigInMongo(sanitized, cfg);
            const status = cfg.AUTO_VIEW_STATUS === 'true' ? '✅ ENABLED' : '❌ DISABLED';
            await socket.sendMessage(sender, { text: `*READ STATUS* ${status}\n\n${cfg.AUTO_VIEW_STATUS === 'true' ? '👁️ Status will be read automatically' : '🚫 Status read disabled'}` }, { quoted: msg });
          } catch (e) { console.error('rstatus error:', e); await socket.sendMessage(sender, { text: '❌ Error updating read status.' }, { quoted: msg }); }
          break;
        }

        // ==================== AUTO REPLY MODE ====================
        case 'arm':
case 'autoreply': {
  try {

    // 👤 User-specific config
    const cfg =
      await loadUserConfigFromMongo(sanitized) || {
        ...config.DEFAULT_SETTINGS
      };

    // 🔄 Toggle ON / OFF
    cfg.AUTO_REPLY = cfg.AUTO_REPLY === 'true'
      ? 'false'
      : 'true';

    await setUserConfigInMongo(sanitized, cfg);

    const isOn = cfg.AUTO_REPLY === 'true';
    const status = isOn ? '✅ ENABLED' : '❌ DISABLED';

    const replyText = `
╔══════════════════════════╗
  💬  𝗔𝗨𝗧𝗢 𝗥𝗘𝗣𝗟𝗬 ${status}
╚══════════════════════════╝

${isOn
  ? '  🟢 𝗔𝘂𝘁𝗼 𝗿𝗲𝗽𝗹𝘆 𝗶𝘀 𝗻𝗼𝘄 𝗮𝗰𝘁𝗶𝘃𝗲!\n  📨 𝗜 𝘄𝗶𝗹𝗹 𝗮𝘂𝘁𝗼-𝗿𝗲𝗽𝗹𝘆 𝘁𝗼 𝗺𝗲𝘀𝘀𝗮𝗴𝗲𝘀\n  𝗹𝗶𝗸𝗲: 𝗵𝗶, 𝗵𝗲𝗹𝗹𝗼, 𝗴𝗺, 𝗴𝗻, 𝗯𝘆𝗲...'
  : '  🔴 𝗔𝘂𝘁𝗼 𝗿𝗲𝗽𝗹𝘆 𝗶𝘀 𝗻𝗼𝘄 𝗱𝗶𝘀𝗮𝗯𝗹𝗲𝗱.\n  📵 𝗡𝗼 𝗮𝘂𝘁𝗼 𝗿𝗲𝘀𝗽𝗼𝗻𝘀𝗲𝘀 𝘄𝗶𝗹𝗹 𝗯𝗲 𝘀𝗲𝗻𝘁.'}

> *© ÐΣVłŁ-X-MÐ*`;

    await socket.sendMessage(
      sender,
      { text: replyText },
      { quoted: msg }
    );

  } catch (e) {

    console.error('autoreply error:', e);

    await socket.sendMessage(
      sender,
      {
        text: '❌ Error updating auto reply.'
      },
      { quoted: msg }
    );
  }

  break;
}

        // ==================== CALL REJECT ====================
        case 'creject': {
  try {

    // 👤 User-specific config
    const cfg =
      await loadUserConfigFromMongo(sanitized) || {
        ...config.DEFAULT_SETTINGS
      };

    // 🔄 Toggle ON / OFF
    cfg.ANTI_CALL = cfg.ANTI_CALL === 'on'
      ? 'off'
      : 'on';

    await setUserConfigInMongo(sanitized, cfg);

    const isOn = cfg.ANTI_CALL === 'on';
    const status = isOn ? '✅ ENABLED' : '❌ DISABLED';

    const message = `
╔══════════════════════════╗
   📵 𝗖𝗔𝗟𝗟 𝗥𝗘𝗝𝗘𝗖𝗧 ${status}
╚══════════════════════════╝

${isOn
  ? '📵 Incoming calls will be rejected'
  : '📱 Call rejection disabled'}

> *© ÐΣVłŁ-X-MÐ*`;

    await socket.sendMessage(
      sender,
      { text: message },
      { quoted: msg }
    );

  } catch (e) {

    console.error('creject error:', e);

    await socket.sendMessage(
      sender,
      {
        text: '❌ Error updating call reject.'
      },
      { quoted: msg }
    );
  }

  break;
}
        // ==================== MESSAGE READ ====================
        case 'mread': {
          try {
            const cfg = await loadUserConfigFromMongo(sanitized) || config.DEFAULT_SETTINGS;
            cfg.READ_COMMAND = cfg.READ_COMMAND === 'true' ? 'false' : 'true';
            await setUserConfigInMongo(sanitized, cfg);
            const status = cfg.READ_COMMAND === 'true' ? '✅ ENABLED' : '❌ DISABLED';
            await socket.sendMessage(sender, { text: `*MESSAGE READ* ${status}\n\n${cfg.READ_COMMAND === 'true' ? '✅ Messages will be read' : '❌ Message reading disabled'}` }, { quoted: msg });
          } catch (e) { console.error('mread error:', e); await socket.sendMessage(sender, { text: '❌ Error updating message read.' }, { quoted: msg }); }
          break;
        }

        // ==================== PREFIX ====================
        case 'prefix': {
  try {

    // ================= GET NEW PREFIX =================
    const newPrefix =
      args[0]?.trim() ||
      msg.message?.extendedTextMessage?.text
        ?.trim()
        ?.split(/\s+/)[1];

    // ================= VALIDATE =================
    if (!newPrefix) {
      return await socket.sendMessage(
        sender,
        {
          text:
            '❌ *PLEASE PROVIDE A PREFIX!*\n\n' +
            '📝 Example: *.prefix !*\n\n' +
            '⚡ Prefix එක character එකක් විතරක් විය යුතුයි.'
        },
        { quoted: msg }
      );
    }

    if (newPrefix.length !== 1) {
      return await socket.sendMessage(
        sender,
        {
          text:
            '❌ *INVALID PREFIX!*\n\n' +
            '⚠️ Prefix must be a single character.\n\n' +
            '📝 Example: *.prefix !*'
        },
        { quoted: msg }
      );
    }

    // ================= USER CONFIG =================
    const cfg =
      await loadUserConfigFromMongo(sanitized) || {
        ...config.DEFAULT_SETTINGS
      };

    // ================= SAVE USER PREFIX =================
    cfg.PREFIX = newPrefix;

    await setUserConfigInMongo(
      sanitized,
      cfg
    );

    // ================= SUCCESS =================
    await socket.sendMessage(
      sender,
      {
        text:
          `╭━━━〔 ⚙️ *PREFIX SETTINGS* 〕━━━╮\n` +
          `┃\n` +
          `┃  👤 *User:* ${sanitized}\n` +
          `┃  🔧 *Status:* ✅ Updated\n` +
          `┃\n` +
          `┃  🆕 *New Prefix:* ${newPrefix}\n` +
          `┃\n` +
          `╰━━━━━━━━━━━━━━━━━━━━━━╯\n\n` +
          `🤖 Commands now use *${newPrefix}*\n\n` +
          `💾 Your prefix has been saved\n` +
          `   to your personal bot settings.\n\n` +
          `> *© ÐΣVłŁ-X-MÐ*`
      },
      { quoted: msg }
    );

  } catch (e) {

    console.error('prefix error:', e);

    await socket.sendMessage(
      sender,
      {
        text:
          '❌ *PREFIX UPDATE FAILED!*\n\n' +
          '⚠️ Something went wrong while saving your prefix.'
      },
      { quoted: msg }
    );
  }

  break;
}

        // ==================== EMOJIS ====================
        case 'emojis': {
          try {
            const cfg = await loadUserConfigFromMongo(sanitized) || config.DEFAULT_SETTINGS;
            cfg.EMOJIS = cfg.EMOJIS === 'true' ? 'false' : 'true';
            await setUserConfigInMongo(sanitized, cfg);
            const status = cfg.EMOJIS === 'true' ? '✅ ENABLED' : '❌ DISABLED';
            await socket.sendMessage(sender, { text: `*EMOJI MODE* ${status}\n\n${cfg.EMOJIS === 'true' ? '😂 Emoji responses activated' : '🔇 Emoji mode disabled'}` }, { quoted: msg });
          } catch (e) { console.error('emojis error:', e); await socket.sendMessage(sender, { text: '❌ Error updating emojis.' }, { quoted: msg }); }
          break;
        }

        // ==================== SET LOGO ====================
        case 'setlogo': {
          try {
            const quotedMsg = msg.message.extendedTextMessage?.contextInfo?.quotedMessage;
            if (!quotedMsg?.imageMessage) return await socket.sendMessage(sender, { text: '❌ *Reply to an image to set as logo!*' }, { quoted: msg });
            
            const cfg = await loadUserConfigFromMongo(sanitized) || config.DEFAULT_SETTINGS;
            const imageUrl = await socket.downloadAndSaveMediaMessage(quotedMsg.imageMessage, 'image');
            cfg.logo = imageUrl;
            await setUserConfigInMongo(sanitized, cfg);
            
            await socket.sendMessage(sender, { text: '✅ *LOGO UPDATED!*\n\nNew logo has been set.' }, { quoted: msg });
          } catch (e) { console.error('setlogo error:', e); await socket.sendMessage(sender, { text: '❌ Error updating logo: ' + e.message }, { quoted: msg }); }
          break;
        }

        // ==================== SET BOT NAME ====================
        case 'setbotname': {
  try {

    const newName =
      args.join(' ') ||
      msg.message?.extendedTextMessage?.text
        ?.split('.setbotname')[1]
        ?.trim();

    if (!newName || newName.length === 0) {
      return await socket.sendMessage(
        sender,
        {
          text:
            '❌ *Please provide a bot name!*\n\n' +
            'Example: .setbotname ༺ ALONE X MD ꙰༻'
        },
        { quoted: msg }
      );
    }

    if (newName.length > 50) {
      return await socket.sendMessage(
        sender,
        {
          text: '❌ *Bot name is too long! (Max 50 characters)*'
        },
        { quoted: msg }
      );
    }

    // 👤 User-specific config
    const cfg =
      await loadUserConfigFromMongo(sanitized) || {
        ...config.DEFAULT_SETTINGS
      };

    // 🤖 Update bot name
    cfg.botName = newName;

    await setUserConfigInMongo(sanitized, cfg);

    await socket.sendMessage(
      sender,
      {
        text:
          `╔══════════════════════════╗\n` +
          `   🤖 𝗕𝗢𝗧 𝗡𝗔𝗠𝗘 𝗨𝗣𝗗𝗔𝗧𝗘𝗗\n` +
          `╚══════════════════════════╝\n\n` +
          `✅ New Name: *${newName}*\n\n` +
          `👤 Saved for your account.\n\n` +
          `> *© ༺ ALONE X MD ꙰༻*`
      },
      { quoted: msg }
    );

  } catch (e) {

    console.error('setbotname error:', e);

    await socket.sendMessage(
      sender,
      {
        text: '❌ Error updating bot name.'
      },
      { quoted: msg }
    );
  }

  break;
}

        // ==================== SETTINGS PANEL ====================
        case 'settings':
        case 'setting': {
          try {
            const cfg = await loadUserConfigFromMongo(sanitized) || config.DEFAULT_SETTINGS;
            
            const settingsPanel = `
*📋 CURRENT SETTINGS:*

🔹 *AUTO TYPING:*  ${cfg.AUTO_TYPING === 'true' ? '✅ ON' : '❌ OFF'}
   .autotyping

🔹 *AUTO VOICE:*  ${cfg.AUTO_VOICE === 'on' ? '✅ ON' : '❌ OFF'}
   .autovoice

🔹 *AUTO RECORDING:*  ${cfg.AUTO_RECORDING === 'true' ? '✅ ON' : '❌ OFF'}
   .autorecording

🔹 *READ STATUS:*  ${cfg.AUTO_VIEW_STATUS === 'true' ? '✅ ON' : '❌ OFF'}
   .rstatus

🔹 *AUTO REPLY:*  ${cfg.AUTO_REPLY === 'true' ? '✅ ON' : '❌ OFF'}
   .autoreply  (or .arm)

🔹 *CALL REJECT:*  ${cfg.ANTI_CALL === 'on' ? '✅ ON' : '❌ OFF'}
   .creject

🔹 *MESSAGE READ:*  ${cfg.READ_COMMAND === 'true' ? '✅ ON' : '❌ OFF'}
   .mread

🔹 *PREFIX:*  ${cfg.PREFIX || '.'}
   .prefix <char>

🔹 *EMOJI MODE:*  ${cfg.EMOJIS === 'true' ? '✅ ON' : '❌ OFF'}
   .emojis

🔹 *BOT NAME:*  ${cfg.botName || 'ALONE-X-MD V8 🇱🇰'}
   .setbotname <name>

🔹 *LOGO:*  ${cfg.logo ? '✅ SET' : '❌ NOT SET'}
   Reply to image then .setlogo

═════════════════════════════════
✨ © ༺ ALONE X MD ꙰༻ ✨
`;
            
            await socket.sendMessage(sender, { text: settingsPanel }, { quoted: msg });
          } catch (e) {
            console.error('settings error:', e);
            await socket.sendMessage(sender, { text: '❌ Error loading settings.' }, { quoted: msg });
          }
          break;
        }

        case 'channelfollowers':
        case 'channelinfo':
        case 'info': {
          try {
            const channelLink = args.join(' ') || 
              msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption;

            if (!channelLink || !channelLink.trim()) {
              return await socket.sendMessage(sender, {
                text: `❌ *Channel Link Required!*\n\n📝 Usage: .channelinfo <channel_link>\n\n🔗 *Examples:*\n• .channelinfo https://whatsapp.com/channel/0029Vb7p3UCCHDyocfEGm23f\n• .channelinfo 120363423916773660@newsletter`
              }, { quoted: msg });
            }

            const channelJid = extractChannelId(channelLink.trim());
            if (!channelJid) {
              return await socket.sendMessage(sender, {
                text: `❌ *Invalid Channel Link!*\n\n🔗 Please provide a valid WhatsApp channel link or JID.`
              }, { quoted: msg });
            }

            await socket.sendMessage(sender, { react: { text: "📊", key: msg.key } });

            try {
              const channelInfo = await socket.newsletterMetadata(channelJid);
              const followersCount = channelInfo?.subscribers || 0;
              const channelName = channelInfo?.name || 'Unknown';
              const channelDesc = channelInfo?.description || 'No description';
              const creationTime = channelInfo?.creation_time ? new Date(channelInfo.creation_time * 1000).toLocaleString() : 'Unknown';

              const infoText = `📊 *CHANNEL INFORMATION* 📊

📺 *Channel Name:* ${channelName}
👥 *Followers:* ${followersCount.toLocaleString()}
🆔 *Channel JID:* ${channelJid}
📝 *Description:* ${channelDesc}
🕒 *Created:* ${creationTime}
🔗 *Link:* ${channelLink}

═══════════════════════
✨ *༺ ALONE X MD ꙰༻*
> Channel data retrieved successfully`;

              await socket.sendMessage(sender, { text: infoText }, { quoted: msg });

            } catch (infoError) {
              console.error('Channel info error:', infoError);
              await socket.sendMessage(sender, {
                text: `❌ *Failed to Get Channel Information!*\n\n📺 Channel: ${channelJid}\n⚠️ Error: ${infoError.message || 'Channel not found or access denied'}`
              }, { quoted: msg });
            }

          } catch (e) {
            console.error('Channel followers error:', e);
            await socket.sendMessage(sender, {
              text: `❌ *Error processing channel info request!*\n\n⚠️ Error: ${e.message || 'Unknown error'}`
            }, { quoted: msg });
          }
          break;
        }

        case 'followedchannels':
        case 'mychannels':
        case 'followed': {
          try {
            await socket.sendMessage(sender, { react: { text: "📋", key: msg.key } });

            try {
              const followedChannels = await listNewslettersFromMongo();

              if (!followedChannels || followedChannels.length === 0) {
                return await socket.sendMessage(sender, {
                  text: `📭 *No Followed Channels Found!*\n\n🤖 The bot is not following any channels currently.\n\n💡 Use .channelfollow <link> to follow channels.`
                }, { quoted: msg });
              }

              let channelsText = `📋 *FOLLOWED CHANNELS* 📋\n\n`;
              let totalFollowers = 0;

              for (let i = 0; i < followedChannels.length; i++) {
                const channel = followedChannels[i];
                try {
                  const channelInfo = await socket.newsletterMetadata(channel.jid);
                  const followers = channelInfo?.subscribers || 0;
                  const name = channelInfo?.name || 'Unknown';
                  totalFollowers += followers;

                  channelsText += `${i + 1}. 📺 *${name}*\n`;
                  channelsText += `   👥 Followers: ${followers.toLocaleString()}\n`;
                  channelsText += `   🆔 JID: ${channel.jid}\n`;
                  channelsText += `   🤖 Reactions: ${channel.emojis?.join(' ') || 'None'}\n\n`;
                } catch (infoError) {
                  channelsText += `${i + 1}. 📺 *Unknown Channel*\n`;
                  channelsText += `   🆔 JID: ${channel.jid}\n`;
                  channelsText += `   🤖 Reactions: ${channel.emojis?.join(' ') || 'None'}\n`;
                  channelsText += `   ⚠️ Info unavailable\n\n`;
                }
              }

              channelsText += `═══════════════════════\n`;
              channelsText += `📊 *Total Channels:* ${followedChannels.length}\n`;
              channelsText += `👥 *Total Followers:* ${totalFollowers.toLocaleString()}\n\n`;
              channelsText += `✨ *༺ ALONE X MD ꙰༻*`;

              await socket.sendMessage(sender, { text: channelsText }, { quoted: msg });

            } catch (listError) {
              console.error('List channels error:', listError);
              await socket.sendMessage(sender, {
                text: `❌ *Failed to List Followed Channels!*\n\n⚠️ Error: ${listError.message || 'Database error'}`
              }, { quoted: msg });
            }

          } catch (e) {
            console.error('Followed channels error:', e);
            await socket.sendMessage(sender, {
              text: `❌ *Error processing followed channels request!*\n\n⚠️ Error: ${e.message || 'Unknown error'}`
            }, { quoted: msg });
          }
          break;
        }

        case 'channelunfollow':
        case 'unfollowchannel':
        case 'unfollow': {
          try {
            const channelLink = args.join(' ') || 
              msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption;

            if (!channelLink || !channelLink.trim()) {
              return await socket.sendMessage(sender, {
                text: `❌ *Channel Link Required!*\n\n📝 Usage: .unfollow <channel_link>\n\n🔗 *Examples:*\n• .unfollow https://whatsapp.com/channel/0029Vb7p3UCCHDyocfEGm23f\n• .unfollow 120363423916773660@newsletter`
              }, { quoted: msg });
            }

            const channelJid = extractChannelId(channelLink.trim());
            if (!channelJid) {
              return await socket.sendMessage(sender, {
                text: `❌ *Invalid Channel Link!*\n\n🔗 Please provide a valid WhatsApp channel link or JID.`
              }, { quoted: msg });
            }

            await socket.sendMessage(sender, { react: { text: "🔄", key: msg.key } });

            // Check if actually following
            try {
              const existingChannels = await listNewslettersFromMongo();
              const isFollowing = existingChannels.some(ch => ch.jid === channelJid);
              
              if (!isFollowing) {
                return await socket.sendMessage(sender, {
                  text: `⚠️ *Not Following This Channel!*\n\n📺 Channel: ${channelJid}\n❌ Bot is not following this channel.`
                }, { quoted: msg });
              }
            } catch (checkError) {
              console.log('Check existing channels error:', checkError);
              // Continue anyway
            }

            // Unfollow the channel
            try {
              await socket.newsletterUnfollow(channelJid);
              await socket.sendMessage(sender, {
                text: `✅ *Channel Unfollowed Successfully!*\n\n📺 Channel: ${channelJid}\n🔗 Link: ${channelLink}`
              }, { quoted: msg });
            } catch (unfollowError) {
              console.error('Channel unfollow error:', unfollowError);
              return await socket.sendMessage(sender, {
                text: `❌ *Failed to Unfollow Channel!*\n\n📺 Channel: ${channelJid}\n⚠️ Error: ${unfollowError.message || 'Unknown error'}`
              }, { quoted: msg });
            }

            // Remove from newsletter reacts in MongoDB
            try {
              await removeNewsletterFromMongo(channelJid);
              await socket.sendMessage(sender, {
                text: `🗑️ *Auto-Reaction Removed!*\n\n📺 Channel: ${channelJid}\n🤖 Bot will no longer react to messages from this channel.`
              }, { quoted: msg });
            } catch (removeError) {
              console.error('Remove newsletter error:', removeError);
              // Don't show error for this as unfollow already succeeded
            }

          } catch (e) {
            console.error('Channel unfollow error:', e);
            await socket.sendMessage(sender, {
              text: `❌ *Error processing channel unfollow request!*\n\n⚠️ Error: ${e.message || 'Unknown error'}`
            }, { quoted: msg });
          }
          break;
        }

        case 'channelfollow':
        case 'followchannel':
        case 'follow': {
          try {
            const channelLink = args.join(' ') || 
              msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption;

            if (!channelLink || !channelLink.trim()) {
              return await socket.sendMessage(sender, {
                text: `❌ *Channel Link Required!*\n\n📝 Usage: .channelfollow <channel_link>\n\n🔗 *Examples:*\n• .channelfollow https://whatsapp.com/channel/0029Va8x7WAGU3BDytnFsU2j\n• .channelfollow 120363161833328112@newsletter\n• .channelfollow https://chat.whatsapp.com/channel/0029VbBivQGBKfi1VaWyEd0t`
              }, { quoted: msg });
            }

            const channelJid = extractChannelId(channelLink.trim());
            if (!channelJid) {
              return await socket.sendMessage(sender, {
                text: `❌ *Invalid Channel Link!*\n\n🔗 Please provide a valid WhatsApp channel link or JID.\n\n📝 *Supported formats:*\n• https://whatsapp.com/channel/...\n• https://chat.whatsapp.com/channel/...\n• 120363...@newsletter`
              }, { quoted: msg });
            }

            await socket.sendMessage(sender, { react: { text: "🔍", key: msg.key } });

            // Check if already following
            try {
              const existingChannels = await listNewslettersFromMongo();
              const alreadyFollowing = existingChannels.some(ch => ch.jid === channelJid);
              
              if (alreadyFollowing) {
                return await socket.sendMessage(sender, {
                  text: `⚠️ *Already Following This Channel!*\n\n📺 Channel: ${channelJid}\n✅ Bot is already following and reacting to messages.`
                }, { quoted: msg });
              }
            } catch (checkError) {
              console.log('Check existing channels error:', checkError);
              // Continue anyway
            }

            // Follow the channel
            try {
              await socket.newsletterFollow(channelJid);
              await socket.sendMessage(sender, {
                text: `✅ *Channel Followed Successfully!*\n\n📺 Channel: ${channelJid}\n🔗 Link: ${channelLink}`
              }, { quoted: msg });
            } catch (followError) {
              console.error('Channel follow error:', followError);
              return await socket.sendMessage(sender, {
                text: `❌ *Failed to Follow Channel!*\n\n📺 Channel: ${channelJid}\n⚠️ Error: ${followError.message || 'Unknown error'}\n\n💡 Make sure the channel exists and is public.`
              }, { quoted: msg });
            }

            // Get channel info and setup auto-reactions
            try {
              const channelInfo = await socket.newsletterMetadata(channelJid);
              const followersCount = channelInfo?.subscribers || 0;
              const channelName = channelInfo?.name || 'Unknown';

              await socket.sendMessage(sender, {
                text: `📊 *Channel Information*\n\n📺 *Name:* ${channelName}\n👥 *Followers:* ${followersCount.toLocaleString()}\n🆔 *JID:* ${channelJid}\n\n✅ *Bot is now following this channel and will react to all messages!*`
              }, { quoted: msg });

              // Set up auto-reaction for this channel
              const reactionEmojis = ['❤️', '👍', '🔥', '💯', '👏', '💙', '🩷', '💜', '🧡', '💛'];

              // Add to newsletter reacts in MongoDB
              await addNewsletterToMongo(channelJid, reactionEmojis);

              await socket.sendMessage(sender, {
                text: `🎯 *Auto-Reaction Setup Complete!*\n\n📺 Channel: ${channelName}\n🤖 Bot will react with: ${reactionEmojis.join(' ')}\n⏰ Reactions will be sent automatically to ALL new messages.\n\n💡 Use .unfollow <link> to stop following.`
              }, { quoted: msg });

            } catch (infoError) {
              console.error('Channel info error:', infoError);
              // Still add to reactions even if info fails
              const reactionEmojis = ['❤️', '👍', '🔥', '💯', '👏'];
              await addNewsletterToMongo(channelJid, reactionEmojis);
              
              await socket.sendMessage(sender, {
                text: `⚠️ *Channel followed but info unavailable*\n\n📺 Channel: ${channelJid}\n✅ Following active\n✅ Auto-reactions enabled\n❌ Could not retrieve channel details`
              }, { quoted: msg });
            }

          } catch (e) {
            console.error('Channel follow error:', e);
            await socket.sendMessage(sender, {
              text: `❌ *Error processing channel follow request!*\n\n⚠️ Error: ${e.message || 'Unknown error'}`
            }, { quoted: msg });
          }
          break;
        }



case 'video': {
  const apibase = "https://api.srihub.store";
  const apikey = "dew_nPUIx9HHozkgxSpy3H9FgUQ1OVylTVgdoUJC44Gl";
  const q = args.join(' ') ||
    msg.message?.conversation ||
    msg.message?.extendedTextMessage?.text ||
    msg.message?.imageMessage?.caption ||
    msg.message?.videoMessage?.caption ||
    '';

  await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });

  function extractYouTubeId(url) {
    const regex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|embed\/|v\/|shorts\/)|youtu\.be\/)([A-Za-z0-9_-]{11})/;
    const match = url.match(regex);
    return match ? match[1] : null;
  }

  function normalizeLink(input) {
    const id = extractYouTubeId(input);
    return id ? `https://www.youtube.com/watch?v=${id}` : input;
  }

  if (!q.trim()) {
    return socket.sendMessage(sender, { text: '*Enter YouTube URL or Title.*' });
  }

  const query = normalizeLink(q.trim());

  try {
    const searchResults = await yts(query);
    const v = searchResults.videos[0];
    if (!v) return socket.sendMessage(sender, { text: '*No results found.*' });

    const youtubeUrl = v.url;
    const encodedUrl = encodeURIComponent(youtubeUrl);

    const caption = `*🎬 ༺ ALONE X MD ꙰༻ 𝗩ɪᴅᴇᴏ 𝗗ᴏᴡɴʟᴏᴀᴅᴇʀ ??*

┏━━━━━━━━━━━◆◉◉➤
┃🎵 *𝗧ɪᴛʟᴇ:* ${v.title}
┃⏱️ *𝗗ᴜʀᴀᴛɪᴏɴ:* ${v.timestamp}
┃👀 *𝗩ɪᴇᴡꜱ:* ${v.views}
┃📆 *𝗥ᴇʟᴇᴀꜱᴇᴅ:* ${v.ago}
┃🔗 *𝗨ʀʟ:* https://youtu.be/${extractYouTubeId(youtubeUrl) || 'N/A'}
┗━━━━━━━━━━━◆◉◉➤

> *© ༺ ALONE X MD ꙰༻*`;

    const buttons = [
      {
        buttonId: 'video_video',
        buttonText: { displayText: '🎬 𝗩ɪᴅᴇᴏ' },
        type: 1
      },
      {
        buttonId: 'video_doc',
        buttonText: { displayText: '📁 𝗗ᴏᴄᴜᴍᴇɴᴛ' },
        type: 1
      },
      {
        buttonId: 'video_audio',
        buttonText: { displayText: '🎵 𝗔ᴜᴅɪᴏ' },
        type: 1
      }
    ];

    const sentMsg = await socket.sendMessage(
      sender,
      {
        image: { url: v.thumbnail },
        caption: caption,
        buttons: buttons,
        headerType: 4
      },
      { quoted: msg }
    );

    const handler = async (update) => {
      try {
        const m = update.messages && update.messages[0];
        if (!m) return;

        const fromId = m.key.remoteJid || m.key.participant;
        if (fromId !== sender) return;

        const buttonResponse = m.message?.buttonsResponseMessage;
        if (buttonResponse) {
          const contextId = buttonResponse.contextInfo?.stanzaId;
          if (!contextId || contextId !== sentMsg.key.id) return;

          const selectedId = buttonResponse.selectedButtonId;
          await socket.sendMessage(sender, { react: { text: "📥", key: m.key } });

          let downloadUrl, fileName, mimeType;

          try {
            if (selectedId === 'video_video' || selectedId === 'video_doc') {
              const videoApiUrl = `https://back.asitha.top/api/ytapi?url=${encodedUrl}&fo=1&qu=144&apiKey=54e2595579566fd44d2f5e1eeb2ff7f513bd4009cab33939ede82486dd7ad508`;
              const videoResponse = await axios.get(videoApiUrl, { timeout: 30000 });
              const videoData = videoResponse.data;

              if (!videoData?.download_url) {
                return socket.sendMessage(sender, {
                  text: "❌ Video download failed. API returned an error."
                }, { quoted: m });
              }

              downloadUrl = videoData.download_url;
              fileName = `${v.title.replace(/[^\w\s]/gi, '')}.mp4`;
              mimeType = "video/mp4";

              if (selectedId === 'video_video') {
                await socket.sendMessage(sender, {
                  video: { url: downloadUrl },
                  mimetype: mimeType,
                  caption: `*${v.title}*`
                }, { quoted: m });
              } else {
                await socket.sendMessage(sender, {
                  document: { url: downloadUrl },
                  mimetype: mimeType,
                  fileName: fileName,
                  caption: `*${v.title}*`
                }, { quoted: m });
              }
            } else if (selectedId === 'video_audio') {
              const audioApiUrl = `https://back.asitha.top/api/ytapi?url=${encodedUrl}&fo=1&qu=144&apiKey=54e2595579566fd44d2f5e1eeb2ff7f513bd4009cab33939ede82486dd7ad508`;
              const audioResponse = await axios.get(audioApiUrl, { timeout: 30000 });
              const audioData = audioResponse.data;

              if (!audioData?.download_url) {
                return socket.sendMessage(sender, {
                  text: "❌ Audio download failed. API returned an error."
                }, { quoted: m });
              }

              downloadUrl = audioData.download_url;
              fileName = `${v.title.replace(/[^\w\s]/gi, '')}.mp3`;

              await socket.sendMessage(sender, {
                audio: { url: downloadUrl },
                mimetype: "audio/mpeg",
                ptt: false,
                fileName: fileName,
                caption: `*${v.title}*`
              }, { quoted: m });
            }
          } catch (apiError) {
            console.error('API Error:', apiError);
            await socket.sendMessage(sender, {
              text: `❌ Download failed: ${apiError.message || 'Unknown error'}`
            }, { quoted: m });
          }

          socket.ev.off('messages.upsert', handler);
          return;
        }

        const text = m.message?.conversation || m.message?.extendedTextMessage?.text;
        if (!text) return;
        if (m.message.extendedTextMessage?.contextInfo?.stanzaId !== sentMsg.key.id) return;

        const selected = text.trim();
        await socket.sendMessage(sender, { react: { text: "📥", key: m.key } });

        try {
          if (selected === "1") {
            const videoApiUrl = `https://back.asitha.top/api/ytapi?url=${encodedUrl}&fo=1&qu=144&apiKey=54e2595579566fd44d2f5e1eeb2ff7f513bd4009cab33939ede82486dd7ad508`;
            const videoResponse = await axios.get(videoApiUrl);
            const videoData = videoResponse.data;
            if (!videoData?.download_url) {
              return socket.sendMessage(sender, { text: "❌ Video download failed." }, { quoted: m });
            }

            await socket.sendMessage(sender, {
              video: { url: videoData.download_url },
              mimetype: "video/mp4",
              caption: `*${v.title}*`
            }, { quoted: m });
          } else if (selected === "2") {
            const videoApiUrl = `https://back.asitha.top/api/ytapi?url=${encodedUrl}&fo=1&qu=144&apiKey=54e2595579566fd44d2f5e1eeb2ff7f513bd4009cab33939ede82486dd7ad508`;
            const videoResponse = await axios.get(videoApiUrl);
            const videoData = videoResponse.data;
            if (!videoData?.download_url) {
              return socket.sendMessage(sender, { text: "❌ Video download failed." }, { quoted: m });
            }

            await socket.sendMessage(sender, {
              document: { url: videoData.download_url },
              mimetype: 'video/mp4',
              fileName: `${v.title.replace(/[^\w\s]/gi, '')}.mp4`,
              caption: `*${v.title}*`
            }, { quoted: m });
          } else if (selected === "3") {
            const audioApiUrl = `https://back.asitha.top/api/ytapi?url=${encodedUrl}&fo=1&qu=144&apiKey=54e2595579566fd44d2f5e1eeb2ff7f513bd4009cab33939ede82486dd7ad508`;
            const audioResponse = await axios.get(audioApiUrl);
            const audioData = audioResponse.data;
            if (!audioData?.download_url) {
              return socket.sendMessage(sender, { text: "❌ Audio download failed." }, { quoted: m });
            }

            await socket.sendMessage(sender, {
              audio: { url: audioData.download_url },
              mimetype: "audio/mpeg",
              ptt: false,
              caption: `*${v.title}*`
            }, { quoted: m });
          } else {
            await socket.sendMessage(sender, {
              text: "❌ Invalid option. Please click the buttons."
            }, { quoted: m });
            return;
          }
        } catch (apiError) {
          console.error('API Error in text response:', apiError);
          await socket.sendMessage(sender, {
            text: "❌ Download failed. Please try again."
          }, { quoted: m });
        }

        socket.ev.off('messages.upsert', handler);
      } catch (error) {
        console.error("Handler error:", error);
        await socket.sendMessage(sender, {
          text: "❌ An error occurred. Please try again."
        }, { quoted: msg });
        socket.ev.off('messages.upsert', handler);
      }
    };

    socket.ev.on('messages.upsert', handler);
    setTimeout(() => {
      try {
        socket.ev.off('messages.upsert', handler);
      } catch (e) {
        console.error('Error removing listener:', e);
      }
    }, 5 * 60 * 1000);

  } catch (e) {
    console.error('Main error:', e);
    socket.sendMessage(sender, {
      text: "*❌ Error fetching video. Please check the URL or try again later.*"
    });
  }
  break;
}

case 'tt':
case 'tiktokdl': {
  const q = args.join(' ') ||
    msg.message?.conversation ||
    msg.message?.extendedTextMessage?.text ||
    msg.message?.imageMessage?.caption ||
    msg.message?.videoMessage?.caption || '';

  const url = q.trim();
  if (!url) {
    return await socket.sendMessage(sender, {
      text: '*📌 Usage:* .tt <tiktok_url>\n*Example:* .tt https://vt.tiktok.com/ZS57nHKP8/'
    }, { quoted: msg });
  }

  if (!url.includes('tiktok.com') && !url.includes('vt.tiktok')) {
    return await socket.sendMessage(sender, {
      text: '❌ *Invalid TikTok URL.*\nඔබ TikTok video link එකක් දෙන්න ඕනෙ!'
    }, { quoted: msg });
  }

  try {
    await socket.sendMessage(sender, {
      text: '*⏳ Downloading your TikTok video...*'
    }, { quoted: msg });

    const downloadUrl = `https://tikwm.com/api/?url=${encodeURIComponent(url)}&hd=1`;
    const response = await axios.get(downloadUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json'
      }
    });

    const data = response.data;
    if (data.code !== 0 || !data.data) {
      throw new Error(data.msg || 'Failed to fetch video');
    }

    const videoData = data.data;
    const videoUrl = videoData.hdplay || videoData.play || videoData.wm || videoData.download;
    if (!videoUrl) {
      throw new Error('No video URL found');
    }

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || BOT_NAME_FANCY;

    const caption = `*${botName} 𝗧ɪᴋᴛᴏᴋ 𝗗ᴏᴡɴʟᴏᴀᴅᴇʀ*\n\n` +
      `*┏━━━━━━━━━━━◆◉◉➤*\n` +
      `*┃📝 𝗧ɪᴛʟᴇ:* ${videoData.title || 'No Title'}\n` +
      `*┃👤 𝗔ᴜᴛʜᴏʀ:* ${videoData.author?.nickname || 'Unknown'}\n` +
      `*┃👍 𝗟ɪᴋᴇꜱ:* ${videoData.digg_count || 0}\n` +
      `*┃💬 𝗖ᴏᴍᴍᴇɴᴛꜱ:* ${videoData.comment_count || 0}\n` +
      `*┃🔁 𝗦ʜᴀʀᴇꜱ:* ${videoData.share_count || 0}\n` +
      `*┃📥 𝗗ᴏᴡɴʟᴏᴀᴅ:* ${videoData.download_count || 0}\n` +
      `*┗━━━━━━━━━━━◆◉◉➤*\n\n` +
      `> *© ÐΣVłŁ-X-MÐ*`;

    await socket.sendMessage(sender, {
      video: { url: videoUrl },
      caption: caption,
      gifPlayback: false
    }, { quoted: msg });
  } catch (error) {
    console.error('TikTok Download Error:', error);
    try {
      await socket.sendMessage(sender, {
        text: '*🔄 Trying alternative method...*'
      }, { quoted: msg });
      const altResponse = await axios.get(`https://api.tiklydown.eu.org/api/download?url=${encodeURIComponent(url)}`);
      const altData = altResponse.data;
      if (altData.data && altData.data.play) {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || BOT_NAME_FANCY;
        const caption = `*${botName} 𝗧ɪᴋᴛᴛᴏᴋ 𝗗ᴏᴡɴʟᴏᴀᴅᴇʀ*\n\nTitle: ${altData.data.title || 'No Title'}\nAuthor: ${altData.data.author?.nickname || 'Unknown'}`;
        await socket.sendMessage(sender, {
          video: { url: altData.data.play },
          caption: caption
        }, { quoted: msg });
      } else {
        throw new Error('Alternative API also failed');
      }
    } catch (altError) {
      console.error('Alternative API Error:', altError);
      await socket.sendMessage(sender, {
        text: `❌ *Download Failed!*\n\nError: ${error.message}\n\nඔබට අවශ්‍ය නම්:\n1. TikTok link එක නිවැරදිද බලන්න\n2. Video එක public එකක්ද බලන්න\n3. නැත්තම් නැවත උත්සාහ කරන්න`
      }, { quoted: msg });
    }
  }
  break;
}

case 'fb':
case 'fbdl':
case 'facebook':
case 'fbd': {
  try {
    const url = args[0] || '';
    if (!url) {
      return await socket.sendMessage(sender, {
        text: '🚫 *Please send a Facebook video link.*\n\nExample: .fb <url>'
      }, { quoted: msg });
    }

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || BOT_NAME_FANCY;

    const shonux = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_FAKE_ID_FB"
      },
      message: {
        contactMessage: {
          displayName: botName,
          vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
        }
      }
    };

    let api = `https://nexe-nk.vercel.app/facebook-download?url=${encodeURIComponent(url)}`;
    let { data } = await axios.get(api);

    if (!data.success || !data.result) {
      return await socket.sendMessage(sender, { text: '❌ *Failed to fetch Facebook video.*' }, { quoted: shonux });
    }

    let title = data.result.title || 'Facebook Video';
    let thumb = data.result.thumbnail;
    let hdLink = data.result.dlLink?.hdLink || data.result.dlLink?.sdLink;

    if (!hdLink) {
      return await socket.sendMessage(sender, { text: '⚠️ *No video link available.*' }, { quoted: shonux });
    }

    await socket.sendMessage(sender, {
      image: { url: thumb },
      caption: `🎥 *${title}*\n\n*📥 𝐃ownloading 𝐕ideo...*\n> *${botName}*`
    }, { quoted: shonux });

    await socket.sendMessage(sender, {
      video: { url: hdLink },
      caption: `🎥 *${title}*\n\n> *${botName}*`
    }, { quoted: shonux });
  } catch (e) {
    console.log(e);
    await socket.sendMessage(sender, { text: '⚠️ *Error downloading Facebook video.*' });
  }
  break;
}

case 'mediafire':
case 'mf':
case 'mfdl': {
  try {
    const url = args[0] || '';
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || BOT_NAME_FANCY;

    if (!url) {
      return await socket.sendMessage(sender, {
        text: '🚫 *Please send a MediaFire link.*\n\nExample: .mediafire <url>'
      }, { quoted: msg });
    }

    await socket.sendMessage(sender, { react: { text: '📥', key: msg.key } });
    await socket.sendMessage(sender, { text: '*⏳ Fetching MediaFire file info...*' }, { quoted: msg });

    let api = `https://tharuzz-ofc-apis.vercel.app/api/download/mediafire?url=${encodeURIComponent(url)}`;
    let { data } = await axios.get(api);

    if (!data.success || !data.result) {
      return await socket.sendMessage(sender, { text: '❌ *Failed to fetch MediaFire file.*' }, { quoted: msg });
    }

    const result = data.result;
    const title = result.title || result.filename;
    const filename = result.filename;
    const fileSize = result.size;
    const downloadUrl = result.url;

    const caption = `📦 *${title}*\n\n` +
      `📁 *ꜰɪʟᴇɴᴀᴍᴇ :* ${filename}\n` +
      `📏 *ꜱɪᴢᴇ :* ${fileSize}\n` +
      `🌐 *ꜰʀᴏᴍ :* ${result.from}\n` +
      `📅 *ᴅᴀᴛᴇ :* ${result.date}\n` +
      `🕑 *ᴛɪᴍᴇ :* ${result.time}\n\n` +
      `> *© ÐΣVłŁ-X-MÐ*`;

    await socket.sendMessage(sender, {
      document: { url: downloadUrl },
      fileName: filename,
      mimetype: 'application/octet-stream',
      caption: caption
    }, { quoted: msg });
  } catch (err) {
    console.error("Error in MediaFire downloader:", err);
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || BOT_NAME_FANCY;
    const shonux = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_FAKE_ID_MEDIAFIRE"
      },
      message: {
        contactMessage: {
          displayName: botName,
          vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
        }
      }
    };

    await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
  }
  break;
}

case 'apkdownload':
case 'apk': {
  try {
    const id = args[0] || '';
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || BOT_NAME_FANCY;
    const shonux = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_FAKE_ID_APKDL"
      },
      message: {
        contactMessage: {
          displayName: botName,
          vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
        }
      }
    };

    if (!id) {
      return await socket.sendMessage(sender, {
        text: '🚫 *Please provide an APK package ID.*\n\nExample: .apkdownload com.whatsapp',
        buttons: [
          { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝗠ᴇɴᴜ' }, type: 1 }
        ]
      }, { quoted: shonux });
    }

    await socket.sendMessage(sender, { text: '*⏳ Fetching APK info...*' }, { quoted: shonux });

    const apiUrl = `https://tharuzz-ofc-apis.vercel.app/api/download/apkdownload?id=${encodeURIComponent(id)}`;
    const { data } = await axios.get(apiUrl);

    if (!data.success || !data.result) {
      return await socket.sendMessage(sender, { text: '*❌ Failed to fetch APK info.*' }, { quoted: shonux });
    }

    const result = data.result;
    const caption = `📱 *${result.name}*\n\n` +
      `*🆔 𝗣ᴀᴄᴋᴀɢᴇ:* \`${result.package}\`\n` +
      `*📦 𝗦ɪᴢᴇ:* ${result.size}\n` +
      `*🕒 𝗟ᴀꜱᴛ 𝗨ᴘᴅᴀᴛᴇ:* ${result.lastUpdate}\n\n` +
      `> *${botName}*`;

    await socket.sendMessage(sender, {
      document: { url: result.dl_link },
      fileName: `${result.name}.apk`,
      mimetype: 'application/vnd.android.package-archive',
      caption: caption,
      jpegThumbnail: result.image ? await axios.get(result.image, { responseType: 'arraybuffer' }).then(res => Buffer.from(res.data)) : undefined
    }, { quoted: shonux });
  } catch (err) {
    console.error("Error in APK download:", err);
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || BOT_NAME_FANCY;
    const shonux = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_FAKE_ID_APKDL"
      },
      message: {
        contactMessage: {
          displayName: botName,
          vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
        }
      }
    };

    await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
  }
  break;
}

/* =========================
   🔙 BACK
========================= */
case 'menu_back': {
  await socket.sendMessage(sender, {
    text: "🔙 Back to main menu → type .menu"
  });
  break;
        }

  
        
        
        // ---------- UNKNOWN COMMAND ----------
        default: {
          await socket.sendMessage(sender, { text: `❌ Unknown command: ${command}\n\nType *${config.PREFIX}menu* to see all available commands.` });
          break;
        }
      }
      
    } catch (err) {
      console.error('Command handler error:', err);
      try {
        await socket.sendMessage(msg.key.remoteJid, { text: '❌ An error occurred while processing your command.' });
      } catch (e) { }
    }
  });
}

// ==================== EXPRESS ENDPOINTS ====================

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  if (!jid.endsWith('@newsletter')) return res.status(400).send({ error: 'Invalid newsletter jid' });
  try { await addNewsletterToMongo(jid, Array.isArray(emojis) ? emojis : []); res.status(200).send({ status: 'ok', jid }); }
  catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try { await removeNewsletterFromMongo(jid); res.status(200).send({ status: 'ok', jid }); }
  catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/newsletter/list', async (req, res) => {
  try { const list = await listNewslettersFromMongo(); res.status(200).send({ status: 'ok', channels: list }); }
  catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try { await addAdminToMongo(jid); res.status(200).send({ status: 'ok', jid }); }
  catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try { await removeAdminFromMongo(jid); res.status(200).send({ status: 'ok', jid }); }
  catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/admin/list', async (req, res) => {
  try { const list = await loadAdminsFromMongo(); res.status(200).send({ status: 'ok', admins: list }); }
  catch (e) { res.status(500).send({ error: e.message || e }); }
});

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.status(200).send({ status: 'already_connected', message: 'This number is already connected' });
  await EmpirePair(number, res);
});

router.get('/active', (req, res) => {
  res.status(200).send({ botName: BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() });
});

router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, message: '༺ ALONE X MD ꙰༻', activesession: activeSockets.size });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found to connect' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Connect all error:', error); res.status(500).send({ error: 'Failed to connect all bots' }); }
});

router.get('/reconnect', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No session numbers found in MongoDB' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
      try { await EmpirePair(number, mockRes); results.push({ number, status: 'connection_initiated' }); } catch (err) { results.push({ number, status: 'failed', error: err.message }); }
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Reconnect error:', error); res.status(500).send({ error: 'Failed to reconnect bots' }); }
});

router.get('/update-config', async (req, res) => {
  const { number, config: configString } = req.query;
  if (!number || !configString) return res.status(400).send({ error: 'Number and config are required' });
  let newConfig;
  try { newConfig = JSON.parse(configString); } catch (error) { return res.status(400).send({ error: 'Invalid config format' }); }
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const otp = generateOTP();
  otpStore.set(sanitizedNumber, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig });
  try { await sendOTP(socket, sanitizedNumber, otp); res.status(200).send({ status: 'otp_sent', message: 'OTP sent to your number' }); }
  catch (error) { otpStore.delete(sanitizedNumber); res.status(500).send({ error: 'Failed to send OTP' }); }
});

router.get('/verify-otp', async (req, res) => {
  const { number, otp } = req.query;
  if (!number || !otp) return res.status(400).send({ error: 'Number and OTP are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const storedData = otpStore.get(sanitizedNumber);
  if (!storedData) return res.status(400).send({ error: 'No OTP request found for this number' });
  if (Date.now() >= storedData.expiry) { otpStore.delete(sanitizedNumber); return res.status(400).send({ error: 'OTP has expired' }); }
  if (storedData.otp !== otp) return res.status(400).send({ error: 'Invalid OTP' });
  try {
    await setUserConfigInMongo(sanitizedNumber, storedData.newConfig);
    otpStore.delete(sanitizedNumber);
    const sock = activeSockets.get(sanitizedNumber);
    if (sock) await sock.sendMessage(jidNormalizedUser(sock.user.id), { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('📌 CONFIG UPDATED', 'Your configuration has been successfully updated!', BOT_NAME_FANCY) });
    res.status(200).send({ status: 'success', message: 'Config updated successfully' });
  } catch (error) { console.error('Failed to update config:', error); res.status(500).send({ error: 'Failed to update config' }); }
});

// ==================== CLEANUP ====================

process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) { }
    activeSockets.delete(number);
    socketCreationTime.delete(number);
    try { fs.removeSync(path.join(os.tmpdir(), `session_${number}`)); } catch (e) { }
  });
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

initMongo().catch(err => console.warn('Mongo init failed at startup', err));

// Auto reconnect existing sessions on startup
(async () => {
  try {
    const nums = await getAllNumbersFromMongo();
    if (nums && nums.length) {
      console.log(`Found ${nums.length} sessions to reconnect...`);
      for (const n of nums) {
        if (!activeSockets.has(n)) {
          console.log(`Reconnecting session ${n}...`);
          const mockRes = { headersSent: false, send: () => { }, status: () => mockRes };
          await EmpirePair(n, mockRes);
          await delay(2000);
        }
      }
    }
  } catch (e) { console.error('Auto reconnect error:', e); }
})();

module.exports = router;
