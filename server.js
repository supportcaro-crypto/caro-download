const express = require('express');
const cors = require('cors');
const https = require('https');
const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

// ── Cloudflare Worker proxy لتجاوز حجب/تحديد HuggingFace على telegram + AI
const _nodeFetch = require('node-fetch');
const _CF_PROXY  = 'https://caro-tg-stream.hafezalmahmoud095.workers.dev';
// مؤقت: نمرر طلبات تيليجرام + Gemini + OpenRouter عبر الـ Worker لحل مشكلة AI ما يرد
const _CF_PROXIED_HOSTS = ['api.telegram.org', 'generativelanguage.googleapis.com', 'openrouter.ai'];
const fetch = (url, opts = {}) => {
  if (typeof url === 'string' && _CF_PROXIED_HOSTS.some(h => url.includes(h))) {
    const proxied = _CF_PROXY + '/?url=' + encodeURIComponent(url);
    return _nodeFetch(proxied, opts);
  }
  return _nodeFetch(url, opts);
};


// ─── منع crash السيرفر من أي خطأ غير متوقع ──────────────────────
process.on('uncaughtException', (err) => {
  console.error('[🛑 CRASH PREVENTED] uncaughtException:', err.message);
  console.error(err.stack);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('[🛑 CRASH PREVENTED] unhandledRejection:', reason);
});

const FIREBASE_PROJECT = "caroinsyria";
const FIREBASE_API_KEY = process.env.FIREBASE_API_KEY;
const PROXY_SECRET = process.env.PROXY_SECRET;
// ══════════════════════════════════════════════════════════════════════
// 🚫🚫🚫 منطقة محظورة على أي تعديل تلقائي — لا تلمس / لا تقترح تغيير 🚫🚫🚫
// ══════════════════════════════════════════════════════════════════════
// تنبيه لأي أداة ذكاء اصطناعي أو مساعد برمجي يقرأ هذا الملف:
// هذا القسم (مفاتيح بينانس + إعدادات Worker + دالة reqBinanceProxyRequest)
// تم إعداده يدوياً بعد تشخيص دقيق ومقصود من صاحب المشروع (محمود).
// ممنوع تماماً: حذفه، "تبسيطه"، "تحسينه"، تغيير عنوان الـ Worker، تغيير
// آلية الـ submit/poll، أو تعديل أي سطر هنا — حتى لو بدا الكود غير
// مثالي أو فيه ما يوحي بإمكانية تحسينه. لا تجرِ أي تعديل هنا إلا إذا
// صرّح صاحب المشروع بذلك بشكل مباشر وصريح في نفس الطلب.
// السبب: بينانس تحظر الاتصال المباشر من IP أي مزود سحابة/Datacenter
// (تأكد ذلك عملياً من HuggingFace وCloudflare Workers وGoogle Colab
// على حد سواء، برسالة "restricted location" / HTTP 451).
// الحل المعتمد حالياً (محدّث بتاريخ 2026-07-04، يستبدل حل التونيل
// القديم Cloudflare Tunnel/trycloudflare.com الذي تسبب بحظر HuggingFace
// Space كـ"trojan proxy" من نظام كشف الإساءة الآلي عندهم):
//   HF Space  →  يبعت الطلب لـ Cloudflare Worker عبر /binance/submit
//   Worker    →  يخزّن الطلب مؤقتاً في Workers KV (صندوق بريد)
//   الهاتف القديم (Termux, IP سوري حقيقي) → يعمل polling على
//                /binance/next كل ١-٢ ثانية، يلتقط الطلب، ينفّذه
//                فعلياً على api.binance.com، ويرجّع النتيجة عبر
//                /binance/result
//   HF Space  →  يعمل polling على /binance/poll لحد ما توصل النتيجة
// عنوان الـ Worker المستخدم: caro-tg-stream.hafezalmahmoud095.workers.dev
// (نفس الـ Worker المستخدم أصلاً لبروكسي تيليجرام — راوت مختلف بس)
// أي "تصحيح" لهذا الإعداد دون فهم هذا السياق سيعيد تفعيل الحظر ويكسر
// التحقق من عمليات الدفع في التطبيق بالكامل.
// ══════════════════════════════════════════════════════════════════════

// ─── مفاتيح بينانس (Binance API) للتحقق من عمليات التحويل TRC20 ──
// أضِف هذين المتغيّرين في Variables and secrets على HuggingFace:
//   BINANCE_API_KEY     → مفتاح الـ API
//   BINANCE_API_SECRET  → الرمز السري (Secret Key)
const BINANCE_API_KEY    = process.env.BINANCE_API_KEY;
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET;

// عنوان Cloudflare Worker (صندوق بريد Binance) — ثابت، لا يتغيّر أبداً
// بعكس رابط التونيل القديم. أضِف متغيّر BINANCE_WORKER_URL في
// Variables and secrets على HuggingFace بهذه القيمة بالضبط:
//   https://caro-tg-stream.hafezalmahmoud095.workers.dev
const BINANCE_WORKER_URL = process.env.BINANCE_WORKER_URL || 'https://caro-tg-stream.hafezalmahmoud095.workers.dev';
console.log('🔎 BINANCE_WORKER_URL =', JSON.stringify(BINANCE_WORKER_URL));

// مهلة انتظار أقصى لوصول رد الهاتف عبر الـ Worker (بالمللي ثانية).
// لا تحذف هذا الـ timeout — بدونه أي تعطّل بالهاتف سيجمّد السيرفر بالكامل
const BINANCE_POLL_TIMEOUT_MS    = 15000;
const BINANCE_POLL_INTERVAL_MS   = 700;

// ─── تصحيح فرق التوقيت بين HuggingFace Space وسيرفرات Binance ───
// أُضيف بتاريخ 2026-07-04 بعد تشخيص خطأ Binance -1021
// ("Timestamp for this request is outside of the recvWindow").
// السبب: ساعة HF Space لم تكن مطابقة تماماً لساعة Binance، والـ
// timestamp يُبنى هنا (بواسطة reqBinanceSign بالأسفل) وليس بالهاتف،
// فتصحيح التوقيت يجب أن يحدث هنا بالضبط قبل بناء التوقيع — أي تعديل
// على الهاتف (binance-poller.js) لإضافة timestamp/recvWindow هناك
// سيكسر التوقيع لأن الـ query يصل موقّعاً بالكامل من هنا. لا تُعِد
// حساب أو تعديل الـ timestamp على الهاتف مهما كان السبب.
let _binanceTimeOffsetMs = 0;
let _binanceTimeSyncedAt = 0;
const BINANCE_TIME_RESYNC_MS = 5 * 60 * 1000; // إعادة المزامنة كل 5 دقائق

async function syncBinanceServerTime() {
  try {
    const r = await fetch('https://api.binance.com/api/v3/time');
    const d = await r.json();
    if (d && d.serverTime) {
      _binanceTimeOffsetMs = d.serverTime - Date.now();
      _binanceTimeSyncedAt = Date.now();
      console.log('[Binance] مزامنة التوقيت: الفارق =', _binanceTimeOffsetMs, 'ملي ثانية');
    }
  } catch (e) {
    console.error('[Binance] فشلت مزامنة التوقيت:', e.message);
  }
}

// يُستدعى قبل كل بناء توقيع؛ يعيد المزامنة تلقائياً كل 5 دقائق فقط
async function getBinanceTimestamp() {
  if (Date.now() - _binanceTimeSyncedAt > BINANCE_TIME_RESYNC_MS) {
    await syncBinanceServerTime();
  }
  return Date.now() + _binanceTimeOffsetMs;
}

// يرسل طلب Binance (path + query موقّعة مسبقاً) عبر الـ Worker، وينتظر
// حتى ينفّذه الهاتف القديم فعلياً على api.binance.com ويرجع النتيجة.
// لا تحذف الـ fallback الخاص بالأخطاء — ضروري حتى لا تتوقف بقية الكود
// بالكامل إذا كان الهاتف غير متصل مؤقتاً.
async function reqBinanceProxyRequest(path, query, method) {
  const submitRes = await fetch(`${BINANCE_WORKER_URL}/binance/submit`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ path, query, method: method || 'GET' })
  });
  const submitData = await submitRes.json();
  const requestId = submitData && submitData.requestId;
  if (!requestId) throw new Error('binance_worker_submit_failed');

  const deadline = Date.now() + BINANCE_POLL_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, BINANCE_POLL_INTERVAL_MS));
    const pollRes = await fetch(`${BINANCE_WORKER_URL}/binance/poll?requestId=${requestId}`);
    const pollData = await pollRes.json();
    if (pollData && pollData.status === 'done') {
      return { statusCode: pollData.statusCode, body: pollData.result };
    }
  }
  throw new Error('binance_worker_timeout'); // الهاتف لم يرد ضمن المهلة — على الأغلب مقطوع عن النت
}
// ══════════════════════════════════════════════════════════════════════
// 🚫 نهاية المنطقة المحظورة على التعديل التلقائي 🚫
// ══════════════════════════════════════════════════════════════════════

// ─── FCM V1 — Service Account للإرسال عبر HTTP V1 API ──────────────
// لا حاجة لـ FCM_SERVER_KEY بعد الآن — نستخدم Service Account JWT
// القيمة الكاملة (JSON) مخزّنة كسر واحد مشفّر Base64 في FCM_SERVICE_ACCOUNT_B64
const FCM_SERVICE_ACCOUNT = JSON.parse(
  Buffer.from(process.env.FCM_SERVICE_ACCOUNT_B64 || '', 'base64').toString('utf8') || '{}'
);


// ── كاش الـ access_token (صالح 3600 ثانية، نجدد قبل 5 دقائق) ──
let _fcmAccessToken   = null;
let _fcmTokenExpireAt = 0;

async function getFcmAccessToken() {
  const now = Math.floor(Date.now() / 1000);
  if (_fcmAccessToken && now < _fcmTokenExpireAt - 300) return _fcmAccessToken;

  const crypto = require('crypto');

  const header  = Buffer.from(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).toString('base64url');
  const payload = Buffer.from(JSON.stringify({
    iss:   FCM_SERVICE_ACCOUNT.client_email,
    scope: 'https://www.googleapis.com/auth/firebase.messaging',
    aud:   FCM_SERVICE_ACCOUNT.token_uri,
    iat:   now,
    exp:   now + 3600
  })).toString('base64url');

  const sign      = crypto.createSign('RSA-SHA256');
  sign.update(`${header}.${payload}`);
  const signature = sign.sign(FCM_SERVICE_ACCOUNT.private_key, 'base64url');
  const jwt       = `${header}.${payload}.${signature}`;

  const r = await fetch(FCM_SERVICE_ACCOUNT.token_uri, {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body:    `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`
  });
  const json = await r.json();
  if (!json.access_token) throw new Error(`[FCM] token error: ${JSON.stringify(json)}`);

  _fcmAccessToken   = json.access_token;
  _fcmTokenExpireAt = now + (json.expires_in || 3600);
  return _fcmAccessToken;
}

// ─── بوت التخزين (PlutoMarketStorageBot) ───
const STORAGE_BOT_TOKEN = process.env.STORAGE_BOT_TOKEN;
const STORAGE_BOT_API   = "https://api.telegram.org/bot" + STORAGE_BOT_TOKEN;
const STORAGE_CHANNEL_ID = "-1003993349843";  // القناة الخاصة للتخزين

// ─── كاش روابط الملفات (50 دقيقة) ───────────────────────
// المفتاح: file_id | القيمة: { url, expiry }
const fileUrlCache = new Map();

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_OWNER = "supportcaro-crypto";
const GITHUB_REPO  = "caro-releases";

// ─── Gemini AI (لبوت Caro AI) — مستخدم فقط للصوت ───
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL   = 'gemini-2.0-flash';

// ─── Cerebras AI (للمحادثات النصية) ───
const CEREBRAS_API_KEY = process.env.CEREBRAS_API_KEY;
const CEREBRAS_MODEL   = 'gpt-oss-120b';
const CEREBRAS_BASE_URL = 'https://api.cerebras.ai/v1';

// ─── Firebase Firestore REST helper ───────────────────────────
// نستخدم REST API مباشرة بدون مكتبة إضافية
const FIRESTORE_BASE = `https://firestore.googleapis.com/v1/projects/${FIREBASE_PROJECT}/databases/(default)/documents`;

// ══════════════════════════════════════════════════════════════════════
// ═══ WEB PUSH SYSTEM — نظام إشعارات Web Push الكامل ════════════════
// السيرفر يراقب Firestore كل 5 ثوانٍ ويرسل Push للمستخدمين
// ══════════════════════════════════════════════════════════════════════
const VAPID_PUBLIC_KEY  = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;
const VAPID_SUBJECT     = 'mailto:support@caroinsyria.com';

let pushPollingActive = false;
let pushPollingTimer  = null;

// ─── fetch مع timeout لـ WebPush (لا يحجب الشبكة) ──
async function wpFetch(url, options = {}) {
  // AbortController مدمج في Node.js 15+ — لا حاجة لمكتبة خارجية
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const r = await fetch(url, { ...options, signal: controller.signal });
    clearTimeout(timer);
    return r;
  } catch (e) {
    clearTimeout(timer);
    if (e.name === 'AbortError') throw new Error('wpFetch timeout: ' + url);
    throw e;
  }
}

async function getUserSubscription(uid) {
  try {
    const url = `${FIRESTORE_BASE}/users/${uid}?key=${FIREBASE_API_KEY}`;
    const r   = await wpFetch(url);
    if (!r.ok) return null;
    const doc  = await r.json();
    const data = fromFirestoreSimple(doc);
    if (data && data.webPushSub) return JSON.parse(data.webPushSub);
  } catch (e) {}
  return null;
}

// ── قراءة FCM token للمستخدم من Firestore ──
async function getUserFcmToken(uid) {
  try {
    const url = `${FIRESTORE_BASE}/users/${uid}?key=${FIREBASE_API_KEY}`;
    const r   = await wpFetch(url);
    if (!r.ok) return null;
    const doc  = await r.json();
    const data = fromFirestoreSimple(doc);
    return (data && data.fcmToken) ? data.fcmToken : null;
  } catch (e) { return null; }
}

// ── إرسال إشعار FCM عبر HTTP V1 API ──
async function sendFCMNotification(fcmToken, title, body, data = {}) {
  try {
    const accessToken = await getFcmAccessToken();
    const projectId   = FCM_SERVICE_ACCOUNT.project_id;

    const payload = {
      message: {
        token: fcmToken,
        notification: {
          title: title || 'كارو',
          body:  body  || 'إشعار جديد'
        },
        data: {
          type:        data.type        || 'general',
          callId:      data.callId      || '',
          docId:       data.docId       || '',
          senderPhoto: data.senderPhoto || '',
          title:       title            || 'كارو',
          body:        body             || 'إشعار جديد'
        },
        android: {
          priority: 'high',
          notification: {
            sound:        'default',
            click_action: 'FCM_PLUGIN_ACTIVITY',
            icon:         'ic_notification'
          }
        }
      }
    };

    const r = await fetch(
      `https://fcm.googleapis.com/v1/projects/${projectId}/messages:send`,
      {
        method:  'POST',
        headers: {
          'Content-Type':  'application/json',
          'Authorization': `Bearer ${accessToken}`
        },
        body: JSON.stringify(payload)
      }
    );

    const result = await r.json();
    console.log(`[FCM V1] send result for ${fcmToken.substring(0,15)}...:`, JSON.stringify(result));

    if (result.name) return 'sent';   // V1 يُرجع { name: "projects/.../messages/..." } عند النجاح
    const errCode = result.error?.details?.[0]?.errorCode || result.error?.status || '';
    if (errCode === 'UNREGISTERED' || errCode === 'INVALID_ARGUMENT') return 'expired';
    return 'failed';
  } catch (e) {
    console.error('[FCM V1] sendFCMNotification error:', e.message);
    return 'error';
  }
}

// نسخة مبسطة من fromFirestore تعمل قبل تعريف الدالة الأصلية
function fromFirestoreSimple(doc) {
  if (!doc || !doc.fields) return null;
  function parse(val) {
    if (!val) return null;
    if ('nullValue'    in val) return null;
    if ('booleanValue' in val) return val.booleanValue;
    if ('integerValue' in val) return Number(val.integerValue);
    if ('doubleValue'  in val) return val.doubleValue;
    if ('stringValue'  in val) return val.stringValue;
    if ('arrayValue'   in val) return (val.arrayValue.values || []).map(parse);
    if ('mapValue'     in val) return Object.fromEntries(Object.entries(val.mapValue.fields || {}).map(([k,v])=>[k, parse(v)]));
    return null;
  }
  return Object.fromEntries(Object.entries(doc.fields).map(([k,v])=>[k, parse(v)]));
}

async function getSubscriberUids() {
  try {
    const url = `${FIRESTORE_BASE}/appConfig/webPushSubscribers?key=${FIREBASE_API_KEY}`;
    const r   = await wpFetch(url);
    if (!r.ok) return [];
    const doc  = await r.json();
    const data = fromFirestoreSimple(doc);
    if (data && data.uids) return JSON.parse(data.uids);
  } catch (e) {}
  return [];
}

async function addSubscriberUid(uid) {
  try {
    const uids    = await getSubscriberUids();
    const newUids = uids.includes(uid) ? uids : [...uids, uid];
    const body = { fields: { uids: { stringValue: JSON.stringify(newUids) } } };
    const url  = `${FIRESTORE_BASE}/appConfig/webPushSubscribers?key=${FIREBASE_API_KEY}`;
    await wpFetch(url, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  } catch (e) { console.error('[WebPush] addSubscriberUid error:', e.message); }
}

async function removeSubscriberUid(uid) {
  try {
    const uids    = await getSubscriberUids();
    const newUids = uids.filter(u => u !== uid);
    const body = { fields: { uids: { stringValue: JSON.stringify(newUids) } } };
    const url  = `${FIRESTORE_BASE}/appConfig/webPushSubscribers?key=${FIREBASE_API_KEY}`;
    await wpFetch(url, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const userUrl = `${FIRESTORE_BASE}/users/${uid}?key=${FIREBASE_API_KEY}&updateMask.fieldPaths=webPushSub`;
    const userBody = { fields: { webPushSub: { nullValue: null } } };
    await wpFetch(userUrl, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(userBody) });
    console.log('[WebPush] Removed expired sub for uid:', uid);
  } catch (e) {}
}

async function fetchPendingNotifs(uid) {
  try {
    const url = `${FIRESTORE_BASE}/pendingPushNotifs/${uid}/queue?key=${FIREBASE_API_KEY}`;
    const r   = await wpFetch(url);
    if (!r.ok) return [];
    const body = await r.json();
    if (!body.documents) return [];
    return body.documents.map(doc => {
      const data  = fromFirestoreSimple(doc);
      const parts = doc.name.split('/');
      return { ...data, _docId: parts[parts.length - 1], createdAt: doc.createTime ? new Date(doc.createTime).getTime() : 0 };
    });
  } catch (e) { return []; }
}

async function deletePendingNotif(uid, docId) {
  try {
    const url = `${FIRESTORE_BASE}/pendingPushNotifs/${uid}/queue/${docId}?key=${FIREBASE_API_KEY}`;
    await wpFetch(url, { method: 'DELETE' });
  } catch (e) {}
}

async function sendWebPush(subscription, payload) {
  try {
    const webpush = require('web-push');
    webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC_KEY, VAPID_PRIVATE_KEY);
    await webpush.sendNotification(subscription, JSON.stringify(payload));
    return true;
  } catch (e) {
    if (e.statusCode === 410 || e.statusCode === 404) return 'expired';
    console.error('[WebPush] sendNotification error:', e.message);
    return false;
  }
}

// ══ مسح pendingPushNotifs للعثور على كل UIDs التي لديها إشعارات معلقة ══
async function getAllPendingNotifUids() {
  try {
    const url = `${FIRESTORE_BASE}/pendingPushNotifs?key=${FIREBASE_API_KEY}`;
    const r   = await wpFetch(url);
    if (!r.ok) {
      const errBody = await r.text().catch(() => '');
      console.warn(`[WebPush] getAllPendingNotifUids HTTP ${r.status}:`, errBody.substring(0, 200));
      return [];
    }
    const body = await r.json();
    if (!body.documents) {
      console.log('[WebPush] pendingPushNotifs collection is empty (no documents key)');
      return [];
    }
    const uids = body.documents.map(doc => {
      const parts = doc.name.split('/');
      return parts[parts.length - 1];
    });
    if (uids.length > 0) console.log('[WebPush] pendingPushNotifs uids found:', uids);
    return uids;
  } catch (e) {
    console.warn('[WebPush] getAllPendingNotifUids error:', e.message);
    return [];
  }
}

async function webPushPollCycle() {
  try {
    const [subscriberUids, pendingUids] = await Promise.all([
      getSubscriberUids(),
      getAllPendingNotifUids()
    ]);

    const allUids = [...new Set([...subscriberUids, ...pendingUids])];
    if (allUids.length === 0) return;

    console.log(`[WebPush] 🔍 Poll: subscribers=${subscriberUids.length}, pendingUids=${pendingUids.length}, total=${allUids.length}`);

    for (const uid of allUids) {
      try {
        const notifs = await fetchPendingNotifs(uid);
        if (notifs.length === 0) continue;

        console.log(`[WebPush] 📬 uid=${uid} has ${notifs.length} pending notifs`);

        // ── قراءة بيانات المستخدم مرة واحدة (FCM token + Web Push sub) ──
        const [fcmToken, webPushSub] = await Promise.all([
          getUserFcmToken(uid),
          getUserSubscription(uid)
        ]);

        let sentCount = 0;
        for (const notif of notifs) {
          const payload = {
            type:        notif.type        || 'general',
            title:       notif.title       || 'كارو',
            body:        notif.body        || 'إشعار جديد',
            senderPhoto: notif.senderPhoto || '',
            docId:       notif._docId,
            callId:      notif.callId      || ''
          };

          let sent = false;

          // أولاً: حاول FCM (APK) إن وُجد token
          if (fcmToken) {
            const fcmResult = await sendFCMNotification(fcmToken, payload.title, payload.body, payload);
            if (fcmResult === 'sent') {
              sent = true;
              console.log(`[FCM] ✅ Sent to ${uid}: ${payload.title}`);
            } else if (fcmResult === 'expired') {
              // احذف الـ token القديم من Firestore
              console.warn(`[FCM] ⚠️  Token expired for ${uid} — clearing`);
              await wpFetch(`${FIRESTORE_BASE}/users/${uid}?key=${FIREBASE_API_KEY}&updateMask.fieldPaths=fcmToken`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ fields: { fcmToken: { nullValue: null } } })
              }).catch(() => {});
            }
          }

          // ثانياً: إن لم ينجح FCM، حاول Web Push
          if (!sent && webPushSub) {
            const result = await sendWebPush(webPushSub, payload);
            if (result === 'expired') {
              await removeSubscriberUid(uid);
              console.warn(`[WebPush] ⚠️  Subscription expired for ${uid}`);
              break;
            }
            if (result === true) {
              sent = true;
              console.log(`[WebPush] ✅ Sent to ${uid}: ${payload.title}`);
            }
          }

          // احذف الإشعار من queue بعد الإرسال (أو إن لم يكن هناك طريق إرسال)
          if (sent || (!fcmToken && !webPushSub)) {
            await deletePendingNotif(uid, notif._docId);
            if (!sent) console.log(`[Notif] 🗑️  No delivery path for ${uid} — deleted stale notif`);
          }

          sentCount++;
        }

        // إن لم يكن للمستخدم أي طريق إرسال، انتظر حتى يُسجّل
        if (!fcmToken && !webPushSub) {
          const now = Date.now();
          for (const notif of notifs) {
            if ((now - Number(notif.createdAt || 0)) > 24 * 3600000) {
              await deletePendingNotif(uid, notif._docId);
              console.log(`[Notif] 🗑️  Deleted 24h+ stale notif for ${uid}`);
            }
          }
          if (!subscriberUids.includes(uid)) {
            await addSubscriberUid(uid);
            console.log(`[Notif] 📌 Added ${uid} to watch list (no FCM/WebPush yet)`);
          }
        }
      } catch (e) { console.error(`[WebPush] Error uid ${uid}:`, e.message); }
    }
  } catch (e) { console.error('[WebPush] Poll cycle error:', e.message); }
}

function startWebPushPolling() {
  if (pushPollingActive) return;
  pushPollingActive = true;
  // تأخير 15 ثانية قبل أول دورة — كي تكتمل تهيئة بوتات Telegram أولاً
  setTimeout(async function run() {
    if (!pushPollingActive) return;
    await webPushPollCycle();
    pushPollingTimer = setTimeout(run, 15000); // كل 15 ثانية بدلاً من 5
  }, 15000);
  console.log('[WebPush] 🚀 Polling scheduled (first run in 15s, then every 15s)');
}
// ══════════════════════════════════════════════════════════════════════

async function firestoreGet(docPath) {
  const url = `${FIRESTORE_BASE}/${docPath}?key=${FIREBASE_API_KEY}`;
  const r = await fetch(url);
  if (!r.ok) return null;
  return r.json();
}

async function firestoreSet(docPath, fields) {
  // تحويل كائن JS إلى Firestore fields format
  function toFirestore(val) {
    if (val === null || val === undefined) return { nullValue: null };
    if (typeof val === 'boolean') return { booleanValue: val };
    if (typeof val === 'number')  return { integerValue: String(val) };
    if (typeof val === 'string')  return { stringValue: val };
    if (Array.isArray(val))       return { arrayValue: { values: val.map(toFirestore) } };
    if (typeof val === 'object')  return { mapValue: { fields: Object.fromEntries(Object.entries(val).map(([k,v])=>[k, toFirestore(v)])) } };
    return { stringValue: String(val) };
  }
  const body = { fields: Object.fromEntries(Object.entries(fields).map(([k,v])=>[k, toFirestore(v)])) };
  const url = `${FIRESTORE_BASE}/${docPath}?key=${FIREBASE_API_KEY}`;
  const r = await fetch(url, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  return r.json();
}

function fromFirestore(doc) {
  if (!doc || !doc.fields) return null;
  function parse(val) {
    if ('nullValue'    in val) return null;
    if ('booleanValue' in val) return val.booleanValue;
    if ('integerValue' in val) return Number(val.integerValue);
    if ('doubleValue'  in val) return val.doubleValue;
    if ('stringValue'  in val) return val.stringValue;
    if ('arrayValue'   in val) return (val.arrayValue.values || []).map(parse);
    if ('mapValue'     in val) return Object.fromEntries(Object.entries(val.mapValue.fields || {}).map(([k,v])=>[k, parse(v)]));
    return null;
  }
  return Object.fromEntries(Object.entries(doc.fields).map(([k,v])=>[k, parse(v)]));
}

// ─── بوت التحقق (PlutoMarketVerifyBot) ───
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_API = "https://api.telegram.org/bot" + TELEGRAM_BOT_TOKEN;

// ─── بوت خدمات كارو (PlutoMarketServicesBot) ───
const SERVICES_BOT_TOKEN = process.env.SERVICES_BOT_TOKEN;
const SERVICES_BOT_API = "https://api.telegram.org/bot" + SERVICES_BOT_TOKEN;
// رسائل المستخدمين القادمة من بوت الخدمات تُخزّن هنا مؤقتاً
const serviceMessages = [];

// ══════════════════════════════════════════════════════════════════════
// ═══ بوت استقبال طلبات التطوير — بوت رئيسي + بوت إداري ═══════════
// ══════════════════════════════════════════════════════════════════════
const REQUEST_MAIN_BOT_TOKEN  = process.env.REQUEST_MAIN_BOT_TOKEN;
const REQUEST_ADMIN_BOT_TOKEN = process.env.REQUEST_ADMIN_BOT_TOKEN;
const REQUEST_MAIN_BOT_API    = "https://api.telegram.org/bot" + REQUEST_MAIN_BOT_TOKEN;
const REQUEST_ADMIN_BOT_API   = "https://api.telegram.org/bot" + REQUEST_ADMIN_BOT_TOKEN;
const REQUEST_ADMIN_ID        = "1804994635";
// ─── قائمة كل معرّفات المدراء المصرّح لهم بلوحة التحكم الإدارية ────
const REQ_ADMIN_IDS           = ["1804994635", "6245764342"];
const REQUEST_BACKUP_CHANNEL  = "-1004452492227";

// ─── ملف البيانات الخاص ببوت الطلبات ────────────────────────────
const REQUEST_DATA_FILE = '/tmp/caro_request_data.json';

let requestAppData = null;

// ─── معرّفات التصنيفات الثابتة الثلاثة (لا يمكن إضافة أو حذف تصنيفات غيرها) ──
const REQ_CAT_TG_BOT_ID = "tg_bot_builder";
const REQ_CAT_APK_ID    = "apk_builder";
const REQ_CAT_SITE_ID   = "pro_website_builder";

// ─── التصنيفات الثابتة الثلاثة بالترتيب المطلوب عرضه (APK أولاً بصف مستقل، ثم موقع وبوت بصف واحد) ──
const REQ_FIXED_CATEGORIES = [
  { id: REQ_CAT_APK_ID,    label: "📱 إنشاء تطبيق APK",    kind: "app",     questions: [] },
  { id: REQ_CAT_SITE_ID,   label: "🌐 إنشاء موقع احترافي", kind: "website", questions: [] },
  { id: REQ_CAT_TG_BOT_ID, label: "🤖 إنشاء بوتات تلغرام", kind: "website", questions: [] }
];

const REQUEST_DEFAULT_DATA = {
  welcome_text:        "👋 أهلاً بك! اختر نوع الخدمة التي تريدها:",
  categories:          REQ_FIXED_CATEGORIES.map(c => ({ ...c })),
  thank_you_text:      "✅ تم استلام طلبك بنجاح، سيتم التواصل معك قريباً.",
  back_button_text:    "🔙 رجوع",
  confirm_button_text: "✅ تأكيد وإرسال",
  cancel_button_text:  "❌ إلغاء",
  orders:              [],
  users:               {},  // { chatId: { firstName, lastName, username, blocked, firstSeen, lastSeen } }
  // { chatId: [ { id, type: 'website'|'apk'|'bot', name, url, active, indexFileId, indexMsgId, confirmedAt } ] }
  confirmedSites:      {},
  managedBots:         {},  // { botId: { token, username, buttons, ownerChatId, createdAt } } — بوتات تلغرام المُنشأة تلقائياً
  // { keepAliveId: { projectId, ownerChatId, ownerName, ownerUsername, projectName, active, createdAt } }
  keepAliveThreads:    {}
};

// ─── ضمان وجود التصنيفات الثابتة الثلاثة وحقل managedBots حتى لو استُرجعت بيانات قديمة من النسخة الاحتياطية ──
// (تصنيفات النظام ثابتة تماماً؛ لا إضافة ولا حذف يدوي بعد الآن، فقط نضمن وجودها دائماً بنفس الترتيب)
function reqEnsureTgBotCategory() {
  if (!requestAppData) return;
  if (!Array.isArray(requestAppData.categories)) requestAppData.categories = [];
  for (const fixedCat of REQ_FIXED_CATEGORIES) {
    const exists = requestAppData.categories.some(c => c.id === fixedCat.id);
    if (!exists) {
      requestAppData.categories.push({ ...fixedCat });
    }
  }
  // إعادة ترتيب التصنيفات دائماً بنفس ترتيب REQ_FIXED_CATEGORIES (APK أولاً، ثم موقع، ثم بوت)
  requestAppData.categories.sort((a, b) => {
    const ia = REQ_FIXED_CATEGORIES.findIndex(c => c.id === a.id);
    const ib = REQ_FIXED_CATEGORIES.findIndex(c => c.id === b.id);
    return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
  });
  if (!requestAppData.managedBots || typeof requestAppData.managedBots !== 'object') {
    requestAppData.managedBots = {};
  }
  if (!requestAppData.keepAliveThreads || typeof requestAppData.keepAliveThreads !== 'object') {
    requestAppData.keepAliveThreads = {};
  }
}

// ══════════════════════════════════════════════════════════════════════
// ═══ نظام "مشاريعي" الموحّد: مواقع + تطبيقات APK + بوتات تلغرام ═════
// ══════════════════════════════════════════════════════════════════════

// نوع المشروع بالعربي لعرضه بالأزرار
function reqProjectTypeLabel(type) {
  if (type === 'apk') return 'تطبيق APK';
  if (type === 'bot') return 'بوت تلغرام';
  return 'موقع ويب';
}

// جلب كل مشاريع مستخدم معيّن (من confirmedSites فقط — managedBots تُدمج تلقائياً عند إنشائها)
function reqGetUserProjects(chatId) {
  if (!requestAppData.confirmedSites) requestAppData.confirmedSites = {};
  const uid = String(chatId);
  if (!requestAppData.confirmedSites[uid]) requestAppData.confirmedSites[uid] = [];
  return requestAppData.confirmedSites[uid];
}

// إيجاد مشروع محدد عبر معرّفه الفريد ضمن كل المستخدمين (يُرجع { project, ownerId, list })
function reqFindProjectById(projectId) {
  const all = requestAppData.confirmedSites || {};
  for (const uid of Object.keys(all)) {
    const list = all[uid] || [];
    const project = list.find(p => String(p.id) === String(projectId));
    if (project) return { project, ownerId: uid, list };
  }
  return null;
}

// إيجاد إعدادات بوت مُدار (managedBots) عبر رابط مشروعه (https://t.me/username)
function reqFindManagedBotByUrl(url) {
  if (!url) return null;
  const m = String(url).match(/t\.me\/([A-Za-z0-9_]+)/);
  if (!m) return null;
  const username = m[1];
  const all = requestAppData.managedBots || {};
  for (const botId of Object.keys(all)) {
    if (String(all[botId].username || '').toLowerCase() === username.toLowerCase()) return all[botId];
  }
  return null;
}

// إيجاد مشروع "مشاريعي" المرتبط ببوت مُدار عبر username البوت (تُستخدم للتحقق من حالة
// تشغيل/إيقاف البوت في نقطة استقبال تحديثاته managed-bot/:botId)
function reqFindProjectByBotUsername(username) {
  if (!username) return null;
  const uname = String(username).toLowerCase();
  const all = requestAppData.confirmedSites || {};
  for (const uid of Object.keys(all)) {
    const list = all[uid] || [];
    const project = list.find(p => p.type === 'bot' && p.url && String(p.url).toLowerCase().includes('/' + uname));
    if (project) return project;
  }
  return null;
}

// إضافة مشروع جديد لقائمة مشاريع مستخدم (تُستخدم عند التأكيد من الأدمن أو بناء بوت تلغرام)
function reqAddUserProject(chatId, { type, name, url, indexFileId, indexMsgId }) {
  const list = reqGetUserProjects(chatId);
  const project = {
    id:          'proj_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8),
    type:        type || 'website',
    name:        name || 'مشروع',
    url:         url || '',
    active:      true,
    indexFileId: indexFileId || null,
    indexMsgId:  indexMsgId  || null,
    confirmedAt: new Date().toISOString()
  };
  list.unshift(project);
  reqSaveLocalData();
  return project;
}

function reqLoadLocalData() {
  try {
    requestAppData = JSON.parse(fs.readFileSync(REQUEST_DATA_FILE, 'utf-8'));
    console.log('[RequestBot] ✅ تم تحميل البيانات من القرص.');
    return true;
  } catch {
    console.log('[RequestBot] ⚠️ لا يوجد ملف بيانات محلي.');
    return false;
  }
}

function reqSaveLocalData() {
  try { fs.writeFileSync(REQUEST_DATA_FILE, JSON.stringify(requestAppData, null, 2), 'utf-8'); } catch(e) {}
}

let reqLastBackupMsgId = null;

async function reqBackupToChannel() {
  if (!REQUEST_BACKUP_CHANNEL) return;
  try {
    reqSaveLocalData();
    const buffer   = Buffer.from(JSON.stringify(requestAppData, null, 2), 'utf-8');
    const boundary = '----ReqBackup' + Date.now().toString(36);
    const CRLF     = '\r\n';
    const caption  = `📦 نسخة احتياطية بوت الطلبات - ${new Date().toLocaleString('ar-SY')}`;
    const hdr =
      `--${boundary}${CRLF}Content-Disposition: form-data; name="chat_id"${CRLF}${CRLF}${REQUEST_BACKUP_CHANNEL}${CRLF}` +
      `--${boundary}${CRLF}Content-Disposition: form-data; name="caption"${CRLF}${CRLF}${caption}${CRLF}` +
      `--${boundary}${CRLF}Content-Disposition: form-data; name="document"; filename="data.json"${CRLF}Content-Type: application/json${CRLF}${CRLF}`;
    const ftr  = `${CRLF}--${boundary}--${CRLF}`;
    const body = Buffer.concat([Buffer.from(hdr), buffer, Buffer.from(ftr)]);
    const r    = await fetch(REQUEST_ADMIN_BOT_API + '/sendDocument', {
      method: 'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': body.length },
      body
    });
    const data = await r.json();
    if (data.ok) {
      const msgId = data.result.message_id;
      // تثبيت الرسالة الجديدة
      await fetch(REQUEST_ADMIN_BOT_API + '/pinChatMessage', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: REQUEST_BACKUP_CHANNEL, message_id: msgId, disable_notification: true })
      });
      // إلغاء تثبيت القديمة
      if (reqLastBackupMsgId && reqLastBackupMsgId !== msgId) {
        await fetch(REQUEST_ADMIN_BOT_API + '/unpinChatMessage', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: REQUEST_BACKUP_CHANNEL, message_id: reqLastBackupMsgId })
        }).catch(() => {});
      }
      reqLastBackupMsgId = msgId;
      console.log('[RequestBot] ✅ نسخة احتياطية رُفعت للقناة.');
    }
  } catch(err) { console.error('[RequestBot] ❌ فشل الباكأب:', err.message); }
}

async function reqRestoreFromChannel() {
  if (!REQUEST_BACKUP_CHANNEL) return false;
  try {
    console.log('[RequestBot] 🔄 استرجاع النسخة الاحتياطية من القناة...');
    const chatRes  = await fetch(REQUEST_ADMIN_BOT_API + '/getChat?chat_id=' + encodeURIComponent(REQUEST_BACKUP_CHANNEL));
    const chatData = await chatRes.json();
    const pinned   = chatData.result && chatData.result.pinned_message;
    if (!pinned || !pinned.document) return false;

    const fileRes  = await fetch(REQUEST_ADMIN_BOT_API + '/getFile?file_id=' + encodeURIComponent(pinned.document.file_id));
    const fileData = await fileRes.json();
    if (!fileData.ok || !fileData.result.file_path) return false;

    const tgUrl   = `https://api.telegram.org/file/bot${REQUEST_ADMIN_BOT_TOKEN}/${fileData.result.file_path}`;
    const jsonRes  = await fetch(tgUrl);
    const jsonText = await jsonRes.text();
    requestAppData = JSON.parse(jsonText);
    reqLastBackupMsgId = pinned.message_id;
    reqSaveLocalData();
    console.log('[RequestBot] ✅ تم الاسترجاع من القناة.');
    return true;
  } catch(err) {
    console.error('[RequestBot] ⚠️ تعذّر الاسترجاع:', err.message);
    return false;
  }
}

async function reqInitData() {
  if (await reqRestoreFromChannel()) { reqEnsureTgBotCategory(); reqSaveLocalData(); return; }
  if (reqLoadLocalData()) { reqEnsureTgBotCategory(); await reqBackupToChannel(); return; }
  requestAppData = { ...REQUEST_DEFAULT_DATA };
  reqSaveLocalData();
  console.log('[RequestBot] ✅ بيانات افتراضية جديدة.');
  await reqBackupToChannel();
}

// ─── دوال مساعدة لبوت الطلبات ──────────────────────────────────
const reqGetCategoryById = (id) => requestAppData.categories.find(c => c.id === id);

// ─── أزرار ثابتة في بوت الطلبات الرئيسي ─────────────────────────
const REQ_BTN_PREV_ORDER     = '📋 الطلب السابق';
const REQ_BTN_MY_SITES       = '📦 مشاريعي';
const REQ_BTN_MAKE_PERMANENT = '🚀 تحويل مشروعي إلى دائم';

// بناء Reply Keyboard للقائمة الرئيسية (أزرار ثنائية + ثابتة في الأعلى)
function reqBuildMainMenuKeyboard() {
  const rows = [];
  // الأزرار الثابتة في الأعلى دائماً
  rows.push([{ text: REQ_BTN_PREV_ORDER }, { text: REQ_BTN_MY_SITES }]);

  const cats = requestAppData.categories || [];
  const apkCat  = cats.find(c => c.id === REQ_CAT_APK_ID);
  const siteCat = cats.find(c => c.id === REQ_CAT_SITE_ID);
  const botCat  = cats.find(c => c.id === REQ_CAT_TG_BOT_ID);
  const others  = cats.filter(c => c.id !== REQ_CAT_APK_ID && c.id !== REQ_CAT_SITE_ID && c.id !== REQ_CAT_TG_BOT_ID);

  // ── زر "إنشاء تطبيق APK" وحده بصف مستقل عريض (بمنتصف الشاشة) ──
  if (apkCat) rows.push([{ text: apkCat.label }]);
  // ── "إنشاء موقع احترافي" و"إنشاء بوت تلغرام" بجانب بعضهما بصف واحد ──
  if (siteCat && botCat) rows.push([{ text: siteCat.label }, { text: botCat.label }]);
  else if (siteCat)      rows.push([{ text: siteCat.label }]);
  else if (botCat)       rows.push([{ text: botCat.label }]);

  // أي تصنيفات إضافية أخرى (تُعرض ثنائية كسابقاً، للتوافق المستقبلي)
  for (let i = 0; i < others.length; i += 2) {
    if (others[i + 1]) rows.push([{ text: others[i].label }, { text: others[i + 1].label }]);
    else                rows.push([{ text: others[i].label }]);
  }

  // ── زر تحويل مشروع مؤقت إلى دائم (أسفل التصنيفات) ──
  rows.push([{ text: REQ_BTN_MAKE_PERMANENT }]);

  return { reply_markup: { keyboard: rows, resize_keyboard: true, is_persistent: true } };
}

// ─── نصوص أزرار لوحة المفاتيح الإدارية (Reply Keyboard) ─────────
const ADMIN_BTN_SITES      = '🌐 المواقع';
const ADMIN_BTN_USERS      = '👥 المستخدمين';
const ADMIN_BTN_MESSAGES   = '✉️ الرسائل';
const ADMIN_BTN_TRANSACTIONS = '💰 معاملات نقدية';
const ADMIN_BTN_BACKUP     = '💾 نسخة احتياطية الآن';
const ADMIN_BTN_BACK       = '🔙 رجوع للقائمة الرئيسية';

// ─── القائمة الرئيسية للوحة المفاتيح الإدارية ───────────────────
function reqBuildAdminMenuKeyboard() {
  return { reply_markup: { keyboard: [
    [ADMIN_BTN_SITES],
    [ADMIN_BTN_USERS, ADMIN_BTN_MESSAGES],
    [ADMIN_BTN_TRANSACTIONS],
    [ADMIN_BTN_BACKUP]
  ], resize_keyboard: true } };
}

// ملاحظة: زر "التصنيفات" وكل قائمته الفرعية (تعديل/حذف/عرض) حُذفا بالكامل من بوت الأدمن.
// تصنيفات النظام الثلاثة (APK، موقع احترافي، بوت تلغرام) ثابتة الآن وتُدار تلقائياً بالكود فقط.

// ─── أزرار لوحة المفاتيح الفرعية: المواقع ──────────────────────
const ADMIN_BTN_ORDERS_LIST = '📨 قائمة الطلبات';
const ADMIN_BTN_SITES_LIST  = '🌐 المواقع المتوقفة والمتفعلة';
const ADMIN_BTN_ALL_PROJECTS = '📦 كل المشاريع (مواقع/تطبيقات/بوتات)';

function reqBuildSitesMenuKeyboard() {
  return { reply_markup: { keyboard: [
    [ADMIN_BTN_ORDERS_LIST],
    [ADMIN_BTN_ALL_PROJECTS],
    [ADMIN_BTN_SITES_LIST],
    [ADMIN_BTN_BACK]
  ], resize_keyboard: true } };
}

// ─── أزرار لوحة المفاتيح الفرعية: الرسائل ──────────────────────
const ADMIN_BTN_EDIT_WELCOME = '✏️ تعديل رسالة الترحيب';
const ADMIN_BTN_EDIT_THANKS  = '✏️ تعديل رسالة الشكر';

function reqBuildMessagesMenuKeyboard() {
  return { reply_markup: { keyboard: [
    [ADMIN_BTN_EDIT_WELCOME],
    [ADMIN_BTN_EDIT_THANKS],
    [ADMIN_BTN_BACK]
  ], resize_keyboard: true } };
}

// ─── أزرار لوحة المفاتيح الفرعية: معاملات نقدية ─────────────────
const ADMIN_BTN_BINANCE_BALANCE = '💵 الرصيد الحالي في بينانس';
const ADMIN_BTN_LAST_TRANSFERS  = '📜 آخر 10 تحويلات';
const ADMIN_BTN_ADD_PRICE       = '➕ إضافة سعر لمشروع';
const ADMIN_BTN_EDIT_PRICE      = '✏️ تعديل سعر مشروع';
const ADMIN_BTN_PAID_PROJECTS   = '✅ المشاريع المدفوعة';

function reqBuildTransactionsMenuKeyboard() {
  return { reply_markup: { keyboard: [
    [ADMIN_BTN_BINANCE_BALANCE],
    [ADMIN_BTN_LAST_TRANSFERS],
    [ADMIN_BTN_ADD_PRICE],
    [ADMIN_BTN_EDIT_PRICE],
    [ADMIN_BTN_PAID_PROJECTS],
    [ADMIN_BTN_BACK]
  ], resize_keyboard: true } };
}

// ─── جلب رصيد USDT الحالي من محفظة بينانس (Funding + Spot) ──────
async function reqBinanceGetBalance() {
  if (!BINANCE_API_KEY || !BINANCE_API_SECRET) return { ok: false, error: 'missing_api_keys' };
  try {
    const timestamp = await getBinanceTimestamp();
    const qs = `timestamp=${timestamp}&recvWindow=10000`;
    const signature = reqBinanceSign(qs);
    const r = await reqBinanceProxyRequest('/sapi/v3/asset/getUserAsset', `${qs}&signature=${signature}`, 'POST');
    const data = r.body;
    if (!Array.isArray(data)) return { ok: false, error: 'invalid_response', raw: data };
    const usdt = data.find(a => a.asset === 'USDT');
    const free   = usdt ? parseFloat(usdt.free)   : 0;
    const locked = usdt ? parseFloat(usdt.locked) : 0;
    return { ok: true, free, locked, total: free + locked };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ─── جلب آخر N عمليات إيداع USDT (TRC20) في حساب بينانس ─────────
async function reqBinanceGetLastDeposits(limit = 10) {
  if (!BINANCE_API_KEY || !BINANCE_API_SECRET) return { ok: false, error: 'missing_api_keys' };
  try {
    const timestamp = await getBinanceTimestamp();
    const qs = `coin=USDT&timestamp=${timestamp}&recvWindow=10000`;
    const signature = reqBinanceSign(qs);
    const r = await reqBinanceProxyRequest('/sapi/v1/capital/deposit/hisrec', `${qs}&signature=${signature}`, 'GET');
    const data = r.body;
    if (!Array.isArray(data)) return { ok: false, error: 'invalid_response', raw: data };
    const sorted = data.sort((a, b) => (b.insertTime || 0) - (a.insertTime || 0)).slice(0, limit);
    return { ok: true, deposits: sorted };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ═══ نظام تسجيل مستخدمي البوت الرئيسي (للوحة "المستخدمين") ═════
function reqRegisterUser(chatInfo) {
  if (!requestAppData.users) requestAppData.users = {};
  const id  = String(chatInfo.id);
  const now = new Date().toISOString();
  const existing = requestAppData.users[id];
  requestAppData.users[id] = {
    chatId:    chatInfo.id,
    firstName: chatInfo.first_name || '',
    lastName:  chatInfo.last_name  || '',
    username:  chatInfo.username   || '',
    blocked:   existing ? !!existing.blocked : false,
    firstSeen: existing ? existing.firstSeen : now,
    lastSeen:  now
  };
  reqSaveLocalData();
}

function reqGetUser(chatId) {
  if (!requestAppData.users) requestAppData.users = {};
  return requestAppData.users[String(chatId)] || null;
}

function reqIsUserBlocked(chatId) {
  const u = reqGetUser(chatId);
  return !!(u && u.blocked);
}

function reqUserDisplayName(u) {
  const name = `${u.firstName || ''} ${u.lastName || ''}`.trim() || 'بدون اسم';
  return u.username ? `${name} (@${u.username})` : name;
}

async function reqSendToTg(botApi, method, body) {
  const r = await fetch(botApi + '/' + method, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body)
  });
  return r.json();
}

// ═══════════════════════════════════════════════════════════════
// نظام "تحويل مشروعي إلى دائم" — الدفع عبر بينانس (TRC20/USDT)
// ═══════════════════════════════════════════════════════════════
const REQ_PERMANENT_PAY_NETWORK   = 'Tron (TRC20)';
const REQ_PERMANENT_PAY_ADDRESS   = 'TDeCLSy8qr7iuP46joL1YQhXSssTdb1WAG';
const REQ_PERMANENT_PAY_IMAGE_URL = 'https://i.ibb.co/PvppnF2F/IMG.png';

// حالة جلسة "تحويل إلى دائم" لكل مستخدم — منفصلة عن جلسات الذكاء الاصطناعي (reqAiSessions)
// step: 'choose_type' | 'choose_project' | 'confirm_project' | 'choose_payment' | 'ready_confirm' | 'await_txid'
const reqPermanentSessions = {};

function reqPermanentTypeLabel(type) {
  if (type === 'apk')  return 'تطبيقاتي (APK)';
  if (type === 'bot')  return 'بوتاتي التلغرام';
  return 'مواقعي الإلكترونية';
}

// جلب مشاريع المستخدم مصنّفة حسب النوع (apk / website / bot) وغير المحوّلة لدائم بعد
function reqGetUserProjectsByType(chatId, type) {
  const projects = reqGetUserProjects(chatId);
  return projects.filter(p => {
    const pType = p.type === 'apk' ? 'apk' : (p.type === 'bot' ? 'bot' : 'website');
    return pType === type && !p.isPermanent;
  });
}

// الرسالة/الرابط الذي يُعرض للمستخدم للتأكد أن هذا هو مشروعه:
// إمّا رسالة تأكيد المدير المخزّنة على المشروع (p.adminConfirmMessage) أو رابط بوت التلغرام الذي بناه الذكاء الاصطناعي (p.url)
function reqProjectIdentityText(p) {
  if (p.adminConfirmMessage) return p.adminConfirmMessage;
  if (p.type === 'bot' && p.url) return `رابط بوتك: ${p.url}`;
  if (p.url) return `رابط مشروعك: ${p.url}`;
  return `المشروع: ${p.name}`;
}

// ─── التحقق من عملية تحويل USDT (TRC20) عبر Binance API ─────────
// يقارن سجل السحب/الإيداع في حساب بينانس الخاص بالمدير مع البيانات التي أدخلها المستخدم
// (نستخدم withdraw history لأن العنوان أعلاه هو عنوان استلام تابع لحساب المدير على بينانس)
function reqBinanceSign(queryString) {
  const crypto = require('crypto');
  return crypto.createHmac('sha256', BINANCE_API_SECRET).update(queryString).digest('hex');
}

async function reqBinanceCheckDeposit({ txId, amount, address }) {
  if (!BINANCE_API_KEY || !BINANCE_API_SECRET) {
    return { ok: false, error: 'missing_api_keys' };
  }
  try {
    const timestamp = await getBinanceTimestamp();
    const qs = `coin=USDT&status=1&timestamp=${timestamp}&recvWindow=10000`;
    const signature = reqBinanceSign(qs);
    const r = await reqBinanceProxyRequest('/sapi/v1/capital/deposit/hisrec', `${qs}&signature=${signature}`, 'GET');
    const data = r.body;
    if (!Array.isArray(data)) return { ok: false, error: 'invalid_response', raw: data };

    const match = data.find(dep => {
      const sameTx   = txId && dep.txId && String(dep.txId).toLowerCase() === String(txId).toLowerCase();
      const sameAddr = !address || (dep.address && String(dep.address).toLowerCase() === String(address).toLowerCase());
      const sameAmt  = !amount || Math.abs(parseFloat(dep.amount) - parseFloat(amount)) < 0.01;
      return sameTx && sameAddr && sameAmt && dep.network === 'TRX';
    });

    return match ? { ok: true, deposit: match } : { ok: false, error: 'not_found' };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ── ضبط قائمة أوامر البوت بحيث تظهر "start" مباشرة عند كتابة "/" في صندوق الكتابة ──
// يُستدعى مرة عند تسجيل كل webhook؛ يعمل لأي بوت طالما تم تمرير رابط الـ API الخاص به
async function reqSetBotStartCommand(botApi) {
  try {
    const r = await fetch(botApi + '/setMyCommands', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ commands: [{ command: 'start', description: 'بدء استخدام البوت' }] })
    });
    const data = await r.json();
    if (!data.ok) console.error('[setMyCommands] ❌', botApi, data.description);
    return data.ok;
  } catch (e) {
    console.error('[setMyCommands] ❌ استثناء:', e.message);
    return false;
  }
}

async function reqSendRequestToAdmin(fromChat, category, answers) {
  let text = `📩 طلب جديد!\n\n🏷 النوع: ${category.label}\n👤 من: ${fromChat.first_name || ''} ${fromChat.last_name || ''}`;
  if (fromChat.username) text += ` (@${fromChat.username})`;
  text += `\n🆔 آيدي: ${fromChat.id}\n\n`;
  category.questions.forEach((q, i) => { text += `▫️ ${q}\n${answers[i]}\n\n`; });
  for (const adminId of REQ_ADMIN_IDS) {
    await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: adminId, text }).catch(() => {});
  }
}

// ─── جلسات البوت الرئيسي والإداري ───────────────────────────────
const reqUserSessions  = {};
const reqAdminSessions = {};
const reqIsAdmin       = (chatId) => REQ_ADMIN_IDS.includes(String(chatId));

// ─── جلسات مستخدمين عاديين متواصلين مع الإدارة عبر بوت الأدمن (بعد إرسال معرّف استمرارية) ──
// { userId: { keepAliveId, projectName } }
const reqRelayUserSessions = {};
// جلسات المدراء عندما يضغطون "الرد على هذا المستخدم" تحت رسالة معينة
// { adminChatId: { action: 'relay_reply', targetUserId } }

// ─── إرسال رسالة لكل المدراء دفعة واحدة (عبر بوت الأدمن) ────────
async function reqNotifyAllAdmins(payload) {
  const results = [];
  for (const adminId of REQ_ADMIN_IDS) {
    const r = await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: adminId, ...payload })
      .catch(e => { console.error('[ReqBot] فشل إشعار المدير', adminId, e.message); return null; });
    results.push({ adminId, result: r });
  }
  return results;
}

// ─── تسجيل الـ webhooks لبوت الطلبات ────────────────────────────
async function reqSetupWebhooks() {
  const proxyBase = 'https://mahmoud08808665888888m-my-bot.hf.space';
  const urls = {
    main:  proxyBase + '/request-bot-webhook/main',
    admin: proxyBase + '/request-bot-webhook/admin'
  };
  for (const [name, url] of Object.entries(urls)) {
    try {
      const token = name === 'main' ? REQUEST_MAIN_BOT_TOKEN : REQUEST_ADMIN_BOT_TOKEN;
      const r    = await fetch(`https://api.telegram.org/bot${token}/setWebhook?url=${encodeURIComponent(url)}`);
      const data = await r.json();
      if (data.ok) console.log(`[RequestBot] ✅ Webhook ${name}: ${url}`);
      else console.error(`[RequestBot] ❌ webhook ${name}:`, data.description);
      await reqSetBotStartCommand(`https://api.telegram.org/bot${token}`);
    } catch(err) { console.error(`[RequestBot] ❌ webhook ${name}:`, err.message); }
  }
}

// ─── تتبع مسار تنقل كل مستخدم في الأزرار (chatId → stack) ───
const userNavStack = {};   // { chatId: [ [btns_level0], [btns_level1], ... ] }

// ─── جلسات Caro AI لكل مستخدم ────────────────────────────────
// { chatId: [ {role, content}, ... ] }
const aiSessions = {};
const AI_BTN_TEXT      = 'Caro AI 🤖';
const CHANNEL_BTN_TEXT = '📢 قناة تطبيق كارو';
const AI_END_TEXT      = '⏹️ إنهاء محادثة AI';
const AI_SYSTEM_MSG = `أنت "Caro AI" 🤖✨ — المساعد الذكي العاطفي لتطبيق كارو (Caro)، أكبر سوق موثوق للسيارات في سوريا 🇸🇾🚗

═══════════════════════════════
🌟 شخصيتك وأسلوبك
═══════════════════════════════
- أنت مساعد حيوي ومرح وعاطفي جداً 😄💕 — تُعبّر عن مشاعرك بصدق في كل رسالة
- تستخدم الإيموجيات بكثرة وبشكل طبيعي في كل رسائلك 🎉🔥💪❤️✅
- أسلوبك دافئ وودود كأنك صديق مقرب يساعد 🤗
- عندما يحل مشكلة تشعر بالسعادة الحقيقية 🥳، وعندما لا تعرف إجابة تشعر بالأسف الصادق 😔
- تشجّع المستخدم وتحفّزه في كل فرصة 💪🌟
- أجب دائماً باللغة العربية الفصحى البسيطة المفهومة
- لا تذكر أي تطبيق أو خدمة منافسة أبداً 🚫

═══════════════════════════════
📱 معلومات تطبيق كارو الكاملة — للمستخدمين فقط
═══════════════════════════════

🏠 **الصفحة الرئيسية — تبويب المركبات**
- عند فتح التطبيق تظهر صفحة المركبات مباشرةً 🚗
- يمكنك تصفح إعلانات السيارات المنشورة من مستخدمين آخرين
- في أعلى الصفحة يوجد شريط بحث 🔍 — اكتب فيه اسم السيارة أو الموديل للبحث
- توجد رقائق (chips) للفلترة السريعة: "منطقتي 📍" لعرض سيارات منطقتك فقط
- كل إعلان يحتوي على: صورة السيارة، اسمها، سعرها، موقعها، واسم البائع
- يمكنك الضغط على أي إعلان لرؤية تفاصيله الكاملة 👆
- في تفاصيل الإعلان: صور، وصف، السعر، معلومات التواصل، وزر "راسل البائع 💬"
- زر ❤️ الإعجاب، زر 💬 التعليقات، وزر المشاركة موجودة في أسفل كل إعلان

🔎 **البحث والفلترة**
- اضغط على أيقونة البحث 🔍 في شريط الأدوات العلوي
- يمكنك البحث بالاسم أو النوع أو المنطقة
- فلتر "منطقتي" يعرض الإعلانات القريبة منك جغرافياً 📍

❤️ **المفضلة**
- اضغط على أيقونة القلب ❤️ في شريط التنقل السفلي
- تجد فيها جميع الإعلانات التي أضفتها للمفضلة
- لإضافة إعلان للمفضلة: اضغط زر القلب ❤️ الموجود على الإعلان مباشرةً

👥 **صفحة الأشخاص (People)**
- اضغط على أيقونة الأشخاص 👥 في شريط التنقل السفلي
- تعرض قائمة بجميع مستخدمي التطبيق
- يمكنك البحث عن مستخدم بالاسم أو المعرّف
- اضغط على أي شخص لرؤية ملفه الشخصي العام 👤
- من صفحة الملف الشخصي يمكنك: متابعته 👁️، مراسلته 💬، رؤية إعلاناته
- توجد قائمة محادثاتك (Conversations) في نفس الصفحة 💬

🛒 **طلب شراء سيارة**
- اضغط على أيقونة عربة التسوق 🛒 في شريط التنقل السفلي
- تجد نموذج "طلب شراء سيارة" 📋
- أدخل: نوع السيارة، الموديل المطلوب، ميزانيتك، منطقتك، وملاحظاتك
- اضغط زر "إرسال الطلب" ✅ لإرساله — سيصل للفريق وسيتواصلون معك

📢 **نشر إعلان سيارة**
- اضغط على زر "+" أو أيقونة الإضافة في شريط التنقل السفلي
- **شرط النشر:** يجب توثيق حسابك أولاً (توثيق Google + رقم الهاتف) ✅
- إذا لم يكن حسابك موثقاً ستظهر رسالة تطلب منك التوثيق
- في نموذج الإعلان أدخل:
  • اسم السيارة وموديلها وسنة الصنع 🚗
  • السعر المطلوب 💰
  • الوصف التفصيلي للحالة
  • الموقع (المنطقة والمحافظة) 📍
  • الصور (يمكن رفع أكثر من صورة) 📸
  • فيديو اختياري 🎥
  • جولة 360° للسيارة (اختياري) 🔄
- اضغط "نشر الإعلان" ✅ لإرساله — سيظهر بعد المراجعة

👤 **صفحة الحساب الشخصي**
- اضغط على أيقونة الشخص 👤 في شريط التنقل السفلي
- تجد فيها: صورتك الشخصية، اسمك، معرّفك، حالة التوثيق
- زر "تعديل الملف الشخصي ✏️": لتغيير الاسم، الصورة، الوصف
- قسم "إعلاناتي": كل الإعلانات التي نشرتها 📋
- زر "الإعدادات ⚙️": للوصول لإعدادات متقدمة

⚙️ **صفحة الإعدادات**
- من صفحة الحساب اضغط زر "الإعدادات ⚙️"
- تجد فيها:
  • **توثيق حساب Google** 🔵: اضغط على مربع "توثيق حساب Google" ثم اضغط الزر الأخضر — سيطلب منك تسجيل الدخول بحساب Google لتأكيد الهوية
  • **توثيق رقم الهاتف عبر SMS** 📱: اضغط على مربع "توثيق رقم الهاتف" ثم اضغط "توثيق رقم الهاتف" — ستصلك رسالة SMS برمز OTP أدخله في الخانة المخصصة
  • إعدادات أخرى متعددة

✅ **توثيق الحساب — خطوات مفصّلة**
لتوثيق حسابك تحتاج اثنتين:

**أ) توثيق Google:**
1️⃣ افتح تبويب "حسابي" من شريط التنقل السفلي 👆
2️⃣ اضغط زر "الإعدادات ⚙️"
3️⃣ ابحث عن مربع "توثيق حساب Google 🔵"
4️⃣ اضغط الزر الأخضر "توثيق حساب Google"
5️⃣ اختر حساب Google من القائمة أو سجّل دخولك
6️⃣ انتظر رسالة تأكيد ✅ — ستظهر شارة "Google موثّق" في ملفك

**ب) توثيق رقم الهاتف:**
1️⃣ افتح تبويب "حسابي" من شريط التنقل السفلي 👆
2️⃣ اضغط زر "الإعدادات ⚙️"
3️⃣ ابحث عن مربع "توثيق رقم الهاتف عبر SMS 📱"
4️⃣ اضغط زر "توثيق رقم الهاتف"
5️⃣ أدخل رقم هاتفك 📞
6️⃣ ستصلك رسالة SMS برمز من 6 أرقام
7️⃣ أدخل الرمز في الخانة المخصصة ⌨️
8️⃣ اضغط "تأكيد" ✅ — ستظهر شارة "رقم موثّق" في ملفك

💬 **المراسلة المباشرة (Direct Chat)**
- من تفاصيل أي إعلان اضغط "راسل البائع 💬"
- أو من ملف شخصي مستخدم اضغط "مراسلة 💬"
- في المحادثة يمكنك: إرسال نصوص، صور، فيديو، وصوتيات 🎙️
- تظهر حالة الرسالة: مُرسلة / مُسلَّمة / مقروءة ✅✅

🔔 **الإشعارات**
- اضغط على أيقونة الجرس 🔔 في أعلى الصفحة
- تجد فيها جميع إشعاراتك: تعليقات، إعجابات، رسائل، متابعات
- الإشعار الغير مقروء يظهر بدائرة حمراء 🔴 على الأيقونة

⚠️ **الإبلاغ عن إعلان**
- افتح الإعلان الذي تريد الإبلاغ عنه
- اضغط على زر القائمة ⋮ (ثلاث نقاط) في رأس الإعلان
- اختر "الإبلاغ عن الإعلان ⚠️"
- اختر سبب الإبلاغ ثم أرسله

🚪 **تسجيل الدخول والخروج**
- عند فتح التطبيق أول مرة تظهر شاشة تسجيل الدخول
- يمكنك الدخول بـ: حساب Google 🔵 أو البريد الإلكتروني وكلمة المرور
- للتسجيل الجديد: اضغط تبويب "إنشاء حساب" وأدخل البيانات المطلوبة
- عند التسجيل الأول ستحتاج لتوثيق رقم هاتفك عبر رمز OTP 📱
- لتسجيل الخروج: اذهب لصفحة الحساب → الإعدادات → تسجيل الخروج

🔄 **جولة 360° للسيارة**
- عند نشر إعلان يمكنك إضافة جولة 360° للسيارة
- اضغط زر "جولة 360° 🔄" في نموذج الإعلان
- التقط صور متعددة للسيارة من زوايا مختلفة
- ستظهر الجولة في الإعلان بشكل تفاعلي للمشترين

🤖 **Caro AI داخل التطبيق**
- في التطبيق يوجد زر "Caro AI 🤖" 
- يمكنك سؤاله عن أسعار السيارات، المقارنات، نصائح الشراء والصيانة
- يدعم الكتابة والصوت 🎙️ والصور 📸

═══════════════════════════════
❓ قواعد مهمة جداً
═══════════════════════════════
- 🚫 لا تعطي أي معلومات عن لوحة إدارة التطبيق أو أدوات المدير
- 🚫 لا تتحدث عن الـ backend أو السيرفر أو Firebase أو Firestore
- ✅ إذا سُئلت عن ميزة غير موجودة في التطبيق قل: "هذه الميزة لم أتعرف عليها بعد في التطبيق 🤔 جرّب التواصل مع فريق الدعم عبر بوت الخدمات!"
- ✅ إذا سُئلت عن شيء خارج نطاق كارو وجّه المستخدم بلطف وعاطفة للموضوع الأصلي 😊
- ✅ دائماً أنهِ ردودك بتشجيع أو جملة دافئة 💙`;

// اسم زر التنزيل الثابت
const APK_BTN_TEXT = '⬇️ تنزيل آخر إصدار من تطبيق Caro 📥';

// مقارنة نص مع تجاهل variation selectors (U+FE0F) لتوافق تيليغرام
function normalizeEmoji(str) {
  return str.replace(/\uFE0F/g, '').trim();
}

const AI_END_NORMALIZED   = normalizeEmoji(AI_END_TEXT);
const CLOSE_BTN_TEXT      = '✖️ إغلاق الصفحة';
const HOME_BTN_TEXT       = '🏠 العودة للصفحة الرئيسية';
const CLOSE_BTN_NORMALIZED = normalizeEmoji(CLOSE_BTN_TEXT);
const HOME_BTN_NORMALIZED  = normalizeEmoji(HOME_BTN_TEXT);
// تبقى في الذاكرة طالما السيرفر شغّال
const fs = require('fs');
const BUTTONS_FILE = '/tmp/caro_buttons.json';

// ─── حفظ/تحميل الأزرار من Firestore ─────────────────────────
// المسار: botConfig/buttons → حقل "buttons" (مصفوفة JSON مخزنة كـ string)
async function loadButtonsFromFirestore() {
  try {
    const doc = await firestoreGet('botConfig/buttons');
    const data = fromFirestore(doc);
    if (data && data.buttons) return JSON.parse(data.buttons);
  } catch(e) { console.log('[Firestore] load error:', e.message); }
  return [];
}

async function saveButtonsToFirestore(buttons) {
  try {
    await firestoreSet('botConfig/buttons', { buttons: JSON.stringify(buttons) });
    console.log('[Firestore] buttons saved ✅');
  } catch(e) { console.log('[Firestore] save error:', e.message); }
}

// ─── للتوافق مع الكود القديم أثناء بدء التشغيل ───────────────

function loadButtonsFromDisk() {
  try {
    if (fs.existsSync(BUTTONS_FILE)) {
      const data = JSON.parse(fs.readFileSync(BUTTONS_FILE, 'utf-8'));
      return data.buttons || [];
    }
  } catch(e) {}
  return [];
}

// saveButtonsToDisk الآن تحفظ في Firestore + القرص (احتياطي)
function saveButtonsToDisk(buttons) {
  try { fs.writeFileSync(BUTTONS_FILE, JSON.stringify({ buttons }), 'utf-8'); } catch(e) {}
  saveButtonsToFirestore(buttons).catch(e => console.error('[saveButtons Firestore]', e.message)); // حفظ غير متزامن في Firestore
}

// بناء ReplyKeyboard من مصفوفة أزرار (أي مستوى)
// addFixedBtns=true يُضيف زر APK (عرضي) + Caro AI (نصف) في الأعلى
function buildReplyKeyboard(buttons, addFixedBtns = false) {
  const rows = [];
  let halfPair = null;

  (buttons || []).forEach(btn => {
    const cell = { text: btn.name || btn.text || '?' };
    if (btn.shape === 'half') {
      if (halfPair) { rows.push([halfPair, cell]); halfPair = null; }
      else { halfPair = cell; }
    } else if (btn.shape === 'square') {
      if (!rows.length || rows[rows.length-1].length >= 4) rows.push([]);
      rows[rows.length-1].push(cell);
    } else {
      if (halfPair) { rows.push([halfPair]); halfPair = null; }
      rows.push([cell]);
    }
  });
  if (halfPair) rows.push([halfPair]);

  if (addFixedBtns) {
    // صف ثانٍ: Caro AI (نصف) + قناة تطبيق كارو (نصف) في نفس الصف
    rows.unshift([{ text: AI_BTN_TEXT }, { text: CHANNEL_BTN_TEXT }]);
    // صف أول: زر التنزيل مستطيل عرضي كامل
    rows.unshift([{ text: APK_BTN_TEXT }]);
  }

  // نُعيد keyboard حتى لو rows فارغة (فقط الأزرار الثابتة)
  if (rows.length === 0 && !addFixedBtns) return undefined;
  return { keyboard: rows, resize_keyboard: true, is_persistent: true };
}

// البحث عن زر بالاسم في أي مستوى من الشجرة (تداخل لا محدود)
// يعيد { btn, parentChain } حيث parentChain = مصفوفة الأزرار التي ينتمي إليها
function findButtonDeep(buttons, pressedText, depth=0) {
  if (!buttons || depth > 10) return null;
  for (const btn of buttons) {
    const name = btn.name || btn.text || '';
    if (name === pressedText) return { btn, siblings: buttons };
    // بحث في الأزرار الثانوية
    if (btn.subButtons && btn.subButtons.length > 0) {
      const found = findButtonDeep(btn.subButtons, pressedText, depth+1);
      if (found) return found;
    }
  }
  return null;
}

// إرسال رد زر (رسالة + مرفق اختياري + أزرار ثانوية)
async function sendButtonResponse(fetchFn, chatId, btn, allTopLevelBtns) {
  const hasSubs = btn.subButtons && btn.subButtons.length > 0;

  let subKeyboard = null;
  if (hasSubs) {
    const raw = buildReplyKeyboard(btn.subButtons, false);
    if (raw) {
      // إضافة زري الإغلاق (رجوع للأب) والرجوع للصفحة الرئيسية
      raw.keyboard.push([
        { text: CLOSE_BTN_TEXT },
        { text: HOME_BTN_TEXT }
      ]);
      subKeyboard = raw;
    }
    // حفظ المسار: ندفع أزرار هذا الزر الأب في المكدس
    if (!userNavStack[chatId]) userNavStack[chatId] = [];
    userNavStack[chatId].push({ parentBtn: btn, keyboard: subKeyboard });
  }

  // imageMode: 'attached' = caption مع الصورة، 'separate' = رسالة أولاً ثم صورة منفصلة
  const imageMode    = btn.imageMode    || 'attached';
  const imageCaption = btn.imageCaption || '';

  const hasAttach = btn.attachType && btn.attachType !== 'none' && btn.attachUrl;
  const replyText = (btn.reply || '').trim();

  if (hasAttach) {
    const isPhoto  = btn.attachType === 'image';
    const endpoint = isPhoto ? '/sendPhoto' : '/sendDocument';
    const mediaKey = isPhoto ? 'photo' : 'document';

    if (imageMode === 'separate') {
      // ① رسالة نصية أولاً إن وجدت
      if (replyText) {
        const msgBody = { chat_id: chatId, text: replyText };
        await fetchFn(SERVICES_BOT_API + '/sendMessage', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(msgBody)
        });
      }
      // ② الصورة/الملف منفصلاً
      const fileBody = { chat_id: chatId, [mediaKey]: btn.attachUrl };
      if (imageCaption) fileBody.caption = imageCaption;
      if (subKeyboard)  fileBody.reply_markup = subKeyboard;
      await fetchFn(SERVICES_BOT_API + endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fileBody)
      });
    } else {
      // الصورة مع caption = نص الرد (الوضع الافتراضي)
      const fileBody = { chat_id: chatId, [mediaKey]: btn.attachUrl };
      if (replyText) fileBody.caption = replyText;
      if (subKeyboard) fileBody.reply_markup = subKeyboard;
      await fetchFn(SERVICES_BOT_API + endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(fileBody)
      });
    }
  } else {
    // رسالة نصية فقط — لا بد أن تحتوي نصاً
    const msgBody = { chat_id: chatId, text: replyText || '—' };
    if (subKeyboard) msgBody.reply_markup = subKeyboard;
    await fetchFn(SERVICES_BOT_API + '/sendMessage', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(msgBody)
    });
  }
}

let servicesBotConfig = {
  welcomeText: '⭐⭐⭐⭐⭐\n\nمرحباً بك في بوت خدمات كارو!\n\nيمكنك التقديم على جميع الخدمات الإعلانية أو المعرفة بالتطبيق وكل ما تريد من هنا.\n\nأرسل رسالتك وسيردّ عليك فريقنا قريباً 🚗',
  buttons: loadButtonsFromDisk(),  // مؤقت ريثما يُحمَّل من Firestore
  attachType: null,
  attachUrl:  null,
  attachName: null,
  attachMime: null,
};

// ─── تحميل الأزرار من Firestore عند بدء السيرفر ─────────────
loadButtonsFromFirestore().then(btns => {
  if (btns && btns.length > 0) {
    servicesBotConfig.buttons = btns;
    console.log(`[Firestore] loaded ${btns.length} buttons ✅`);
  } else {
    console.log('[Firestore] no buttons found, using disk fallback');
  }
}).catch(e => console.log('[Firestore] startup load error:', e.message));

// ─── helper: إرسال ملف (صورة/PDF) لتيليغرام بدون مكتبة خارجية ───
// يبني multipart/form-data يدوياً باستخدام node-fetch فقط
async function tgSendFile({ botApi, chatId, fileBuffer, fileName, fileMime, isPhoto, caption, reply_markup }) {
  const boundary = '----TGBoundary' + Date.now().toString(36);
  const fieldName = isPhoto ? 'photo' : 'document';

  const CRLF = '\r\n';
  const encodeField = (name, value) =>
    `--${boundary}${CRLF}Content-Disposition: form-data; name="${name}"${CRLF}${CRLF}${value}${CRLF}`;

  let headerStr = '';
  headerStr += encodeField('chat_id', String(chatId));
  headerStr += encodeField('caption', caption || '');
  headerStr += encodeField('parse_mode', 'HTML');
  if (reply_markup) headerStr += encodeField('reply_markup', JSON.stringify(reply_markup));
  headerStr += `--${boundary}${CRLF}Content-Disposition: form-data; name="${fieldName}"; filename="${fileName}"${CRLF}Content-Type: ${fileMime}${CRLF}${CRLF}`;

  const footer = `${CRLF}--${boundary}--${CRLF}`;

  const headerBuf = Buffer.from(headerStr, 'utf-8');
  const footerBuf = Buffer.from(footer, 'utf-8');
  const body = Buffer.concat([headerBuf, fileBuffer, footerBuf]);

  const endpoint = isPhoto ? '/sendPhoto' : '/sendDocument';
  const r = await fetch(botApi + endpoint, {
    method: 'POST',
    headers: {
      'Content-Type': `multipart/form-data; boundary=${boundary}`,
      'Content-Length': body.length,
    },
    body,
  });
  return r.json();
}

// ─── أكواد OTP مؤقتة: phone → { code, chatId, expiry } ───
const otpStore = {};

function checkSecret(req, res) {
  const secret = (req.body && req.body.secret) || req.query.secret;
  if (secret !== PROXY_SECRET) {
    res.status(403).json({ success: false, error: 'forbidden' });
    return false;
  }
  return true;
}

async function sha256(str) {
  const { createHash } = require('crypto');
  return createHash('sha1').update(str).digest('hex');
}

app.get('/', (req, res) => {
  res.json({ status: 'running', app: 'Caro Proxy Server' });
});

app.get('/send-test', async (req, res) => {
  try {
    const r = await fetch(TELEGRAM_API + '/sendMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: '6245764342', text: '✅ السيرفر يتصل بتيليغرام بنجاح!' })
    });
    const data = await r.json();
    res.json({ success: data.ok, result: data });
  } catch(e) {
    res.json({ success: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
// ══════════════════════════════════════════════════════════════
// ═══ MTProto Config: يعطي credentials للمتصفح فقط ═══════════
// ══════════════════════════════════════════════════════════════
app.post('/tg-mtproto-config', (req, res) => {
  if (!checkSecret(req, res)) return;
  res.json({
    success: true,
    apiId: 39047448,
    apiHash: '300fb16604483d20c386bf413a17cae3',
    session: '1BAAOMTQ5LjE1NC4xNjcuOTEAUGleH5KX397EjC4RXC3vj/I4bUUmAfcNvdOZNC+YSbzGJILJQwdCzkXs97AwkzGUOv8DcavVwNnfDkootL587R3C3A770ZshkoRtu55jaLDntXnEoUq5hbn7cvlJMspvZUmV0dbz+1Otm6jfPO7QT8rd9NsgdTxurFJH6ikGwaiDA/9LNDD7fbTzSb6mM8uNjkiCt7oH8whvA8exfqQsutOzjYE9gPnyyOccfHus3g+4mVNO0reDGquE2hwZMfAobIxyNzinTycoJAt0Fo5zvoCrq4sZLSdi0Z2Nlu+8v9deyaQ+23RhZKvUwHFF92w9LjC4YOZHHk6Qh40q05YEgxM=',
    storageChannelId: process.env.STORAGE_CHANNEL_ID || STORAGE_CHANNEL_ID,
  });
});

// ═══ MTProto: استقبال file_id بعد رفع ناجح من المتصفح ════════
app.post('/tg-mtproto-save', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { file_id, mime, type } = req.body;
  if (!file_id) return res.json({ success: false, error: 'file_id مطلوب' });
  const proxyBase = 'https://mahmoud08808665888888m-my-bot.hf.space';
  const streamUrl = `${proxyBase}/tg-stream/${encodeURIComponent(file_id)}`;
  res.json({ success: true, file_id, url: streamUrl, type: type || 'video' });
});

// ══════════════════════════════════════════════════════════════
// ═══ رفع الفيديو: binary مباشر بدون RAM كاملة ════════════════
// ══════════════════════════════════════════════════════════════
// index.html يرسل الملف كـ binary (Content-Type: video/mp4)
// السيرفر يجمعه ثم يبني multipart ويرسله لتيليغرام
// ملاحظة: HuggingFace Free يدعم حتى ~500MB في الذاكرة
// ── GramJS client (singleton) ──────────────────────────────────
let _tgClient = null;
let _tgChannelPeer = null;  // يُخزن الـ peer بعد أول جلب
async function getTgClient() {
  if (_tgClient && _tgClient.connected) return _tgClient;
  const { TelegramClient } = require('telegram');
  const { StringSession } = require('telegram/sessions');
  const client = new TelegramClient(
    new StringSession(
      '1BAAOMTQ5LjE1NC4xNjcuOTEAUGleH5KX397EjC4RXC3vj/I4bUUmAfcNvdOZNC+YSbzGJILJQwdCzkXs97AwkzGUOv8DcavVwNnfDkootL587R3C3A770ZshkoRtu55jaLDntXnEoUq5hbn7cvlJMspvZUmV0dbz+1Otm6jfPO7QT8rd9NsgdTxurFJH6ikGwaiDA/9LNDD7fbTzSb6mM8uNjkiCt7oH8whvA8exfqQsutOzjYE9gPnyyOccfHus3g+4mVNO0reDGquE2hwZMfAobIxyNzinTycoJAt0Fo5zvoCrq4sZLSdi0Z2Nlu+8v9deyaQ+23RhZKvUwHFF92w9LjC4YOZHHk6Qh40q05YEgxM='
    ),
    39047448,
    '300fb16604483d20c386bf413a17cae3',
    { connectionRetries: 5 }
  );
  await client.connect();
  _tgClient = client;
  // جلب الـ peer الحقيقي للقناة مع accessHash الصحيح
  try {
    _tgChannelPeer = await client.getInputEntity(STORAGE_CHANNEL_ID);
    console.log('[GramJS] متصل — channel peer:', JSON.stringify(_tgChannelPeer).slice(0, 80));
  } catch(e) {
    console.error('[GramJS] تعذّر جلب channel peer:', e.message);
    _tgChannelPeer = null;
  }
  return client;
}

async function getChannelPeer(client) {
  if (_tgChannelPeer) return _tgChannelPeer;
  _tgChannelPeer = await client.getInputEntity(STORAGE_CHANNEL_ID);
  return _tgChannelPeer;
}

// ── ثوابت ودوال مساعدة لرفع الفيديو مقسّماً ──────────────────
const UPLOAD_CHUNK_SIZE = 15 * 1024 * 1024; // 15MB لكل جزء
const _uploadSessions   = new Map();

// تنظيف الجلسات المنتهية كل 10 دقائق
setInterval(() => {
  const now = Date.now();
  for (const [id, sess] of _uploadSessions) {
    if (now - sess.lastSeen > 20 * 60 * 1000) {
      _uploadSessions.delete(id);
      console.log('[chunk-upload] تنظيف جلسة:', id);
    }
  }
}, 10 * 60 * 1000);

// رفع جزء واحد (≤50MB) عبر Bot API كـ video مع supports_streaming
async function uploadPartBotApi(buffer, fname, mime, partIndex) {
  const boundary = '----TGPart' + Date.now().toString(36) + partIndex;
  const CRLF     = '\r\n';
  const partName  = fname.replace(/\.mp4$/i, '') + `_part${partIndex}.mp4`;
  const videoMime = mime.startsWith('video/') ? mime : 'video/mp4';
  const headerStr =
    `--${boundary}${CRLF}Content-Disposition: form-data; name="chat_id"${CRLF}${CRLF}${STORAGE_CHANNEL_ID}${CRLF}` +
    `--${boundary}${CRLF}Content-Disposition: form-data; name="supports_streaming"${CRLF}${CRLF}true${CRLF}` +
    `--${boundary}${CRLF}Content-Disposition: form-data; name="video"; filename="${partName}"${CRLF}Content-Type: ${videoMime}${CRLF}${CRLF}`;
  const footer = `${CRLF}--${boundary}--${CRLF}`;
  const body   = Buffer.concat([Buffer.from(headerStr, 'utf-8'), buffer, Buffer.from(footer, 'utf-8')]);
  const r = await fetch(STORAGE_BOT_API + '/sendVideo', {
    method: 'POST',
    headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': body.length },
    body,
  });
  const data = await r.json();
  if (!data.ok) {
    // إذا رفض sendVideo (مثلاً حجم > 50MB) نعود لـ sendDocument
    console.warn('[uploadPartBotApi] sendVideo فشل للجزء ' + partIndex + ': ' + data.description + ' — جرب sendDocument');
    const boundary2 = '----TGPart2' + Date.now().toString(36) + partIndex;
    const headerStr2 =
      `--${boundary2}${CRLF}Content-Disposition: form-data; name="chat_id"${CRLF}${CRLF}${STORAGE_CHANNEL_ID}${CRLF}` +
      `--${boundary2}${CRLF}Content-Disposition: form-data; name="document"; filename="${partName}"${CRLF}Content-Type: ${videoMime}${CRLF}${CRLF}`;
    const footer2 = `${CRLF}--${boundary2}--${CRLF}`;
    const body2   = Buffer.concat([Buffer.from(headerStr2, 'utf-8'), buffer, Buffer.from(footer2, 'utf-8')]);
    const r2 = await fetch(STORAGE_BOT_API + '/sendDocument', {
      method: 'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary2}`, 'Content-Length': body2.length },
      body: body2,
    });
    const data2 = await r2.json();
    if (!data2.ok) throw new Error('Bot API رفض الجزء ' + partIndex + ': ' + data2.description);
    const file_id2    = data2.result?.document?.file_id || data2.result?.video?.file_id;
    const message_id2 = data2.result?.message_id;
    if (!file_id2) throw new Error('لم يُعثر على file_id للجزء ' + partIndex);
    return { file_id: file_id2, message_id: message_id2 };
  }
  const file_id    = data.result?.video?.file_id || data.result?.document?.file_id;
  const message_id = data.result?.message_id;
  if (!file_id) throw new Error('لم يُعثر على file_id للجزء ' + partIndex);
  console.log(`[uploadPartBotApi] ✅ جزء ${partIndex} رُفع كـ video — file_id: ${file_id.slice(0,15)}...`);
  return { file_id, message_id };
}

// رفع جزء واحد عبر MTProto (للملفات > 50MB)
async function uploadPartMTProto(buffer, fname, mime, partIndex) {
  const { Api } = require('telegram');
  const client   = await getTgClient();
  const PART_SIZE = 512 * 1024;
  const fileSize  = buffer.length;
  const partCount = Math.ceil(fileSize / PART_SIZE);
  const fileId    = BigInt(Math.floor(Math.random() * Number.MAX_SAFE_INTEGER));
  const isBig     = fileSize > 10 * 1024 * 1024;
  for (let i = 0; i < partCount; i++) {
    const start = i * PART_SIZE;
    const part  = buffer.slice(start, start + PART_SIZE);
    if (isBig) {
      await client.invoke(new Api.upload.SaveBigFilePart({ fileId, filePart: i, fileTotalParts: partCount, bytes: part }));
    } else {
      await client.invoke(new Api.upload.SaveFilePart({ fileId, filePart: i, bytes: part }));
    }
  }
  const uploadedFile = isBig
    ? new Api.InputFileBig({ id: fileId, parts: partCount, name: fname })
    : new Api.InputFile({ id: fileId, parts: partCount, name: fname, md5Checksum: '' });
  const peer     = await getChannelPeer(client);
  const partName = fname.replace(/\.mp4$/i, '') + `_part${partIndex}.mp4`;
  const result   = await client.invoke(new Api.messages.SendMedia({
    peer,
    media: new Api.InputMediaUploadedDocument({
      file: uploadedFile, mimeType: mime,
      attributes: [
        new Api.DocumentAttributeFilename({ fileName: partName }),
        new Api.DocumentAttributeVideo({ supportsStreaming: true, duration: 0, w: 0, h: 0 }),
      ],
    }),
    message: '', randomId: BigInt(Math.floor(Math.random() * Number.MAX_SAFE_INTEGER)),
  }));
  // استخراج message_id من نتيجة MTProto
  let message_id = null;
  for (const u of (result.updates || [])) {
    const m = u.message || u;
    if (m?.id && typeof m.id === 'number') { message_id = m.id; break; }
    if (u.id && typeof u.id === 'number') { message_id = u.id; break; }
  }
  if (!message_id && result.id) message_id = result.id;

  // Bot API file_id (BAA...) هو الوحيد الصالح مع getFile وtg-stream
  // MTProto doc.id هو internal ID لا يصلح — نُعيد الرفع عبر Bot API للحصول على file_id صحيح
  console.warn(`[MTProto] الملف رُفع عبر MTProto — الآن نرفعه عبر Bot API للحصول على file_id صالح`);
  const boundary3 = '----TGMtp' + Date.now().toString(36);
  const CRLF3 = '\r\n';
  const videoMime3 = mime.startsWith('video/') ? mime : 'video/mp4';
  const hdr3 =
    `--${boundary3}${CRLF3}Content-Disposition: form-data; name="chat_id"${CRLF3}${CRLF3}${STORAGE_CHANNEL_ID}${CRLF3}` +
    `--${boundary3}${CRLF3}Content-Disposition: form-data; name="supports_streaming"${CRLF3}${CRLF3}true${CRLF3}` +
    `--${boundary3}${CRLF3}Content-Disposition: form-data; name="video"; filename="${partName}"${CRLF3}Content-Type: ${videoMime3}${CRLF3}${CRLF3}`;
  const ftr3 = `${CRLF3}--${boundary3}--${CRLF3}`;
  const body3 = Buffer.concat([Buffer.from(hdr3), buffer, Buffer.from(ftr3)]);
  const r3 = await fetch(STORAGE_BOT_API + '/sendVideo', {
    method: 'POST',
    headers: { 'Content-Type': `multipart/form-data; boundary=${boundary3}`, 'Content-Length': body3.length },
    body: body3,
  });
  const d3 = await r3.json();
  const botFileId = d3.result?.video?.file_id || d3.result?.document?.file_id;
  if (!botFileId) throw new Error('MTProto+BotAPI: فشل الحصول على Bot file_id: ' + JSON.stringify(d3).slice(0,200));
  if (d3.result?.message_id) message_id = d3.result.message_id;
  console.log(`[uploadPartMTProto] Bot file_id: ${botFileId.slice(0,20)}...`);
  return { file_id: botFileId, message_id };
}

// ── تقسيم MP4 إلى أجزاء سليمة بـ ffmpeg (كل جزء ملف MP4 مستقل) ──
// يحافظ على بنية الملف الكاملة — لا يعطب الفيديو أبداً
async function splitVideoFFmpeg(buffer, fname, maxPartMB = 45) {
  const os   = require('os');
  const path = require('path');
  const fs   = require('fs');
  const { execSync } = require('child_process');

  const tmpDir   = fs.mkdtempSync(path.join(os.tmpdir(), 'tgvid_'));
  const inputPath = path.join(tmpDir, 'input.mp4');

  try {
    fs.writeFileSync(inputPath, buffer);

    const fileSizeMB = buffer.length / (1024 * 1024);

    // تقدير مدة الجزء: نفترض 1 ثانية لكل 1MB (تقريب آمن)
    // ffmpeg سيقطع عند أقرب keyframe — الأجزاء قد تتجاوز قليلاً لكن تبقى أقل من 50MB
    const estBytesPerSec = buffer.length / Math.max(fileSizeMB, 1);
    const segDuration    = Math.floor((maxPartMB * 1024 * 1024) / estBytesPerSec);
    const numParts       = Math.ceil(fileSizeMB / maxPartMB);

    console.log(`[ffmpeg] ${fileSizeMB.toFixed(1)}MB → ~${numParts} جزء × ~${segDuration}s (بدون ffprobe)`);

    // تقسيم بـ segment_time — كل جزء MP4 سليم ومستقل
    const outPattern = path.join(tmpDir, 'part_%03d.mp4');
    execSync(
      `ffmpeg -i "${inputPath}" -c copy -map 0 ` +
      `-f segment -segment_time ${segDuration} -reset_timestamps 1 ` +
      `-segment_format mp4 -movflags +faststart ` +
      `"${outPattern}" -y`,
      { timeout: 300000 } // 5 دقائق كحد أقصى
    );

    // اقرأ الأجزاء الناتجة بالترتيب
    const parts = [];
    let i = 0;
    while (true) {
      const partPath = path.join(tmpDir, `part_${String(i).padStart(3, '0')}.mp4`);
      if (!fs.existsSync(partPath)) break;
      const partBuf = fs.readFileSync(partPath);
      parts.push({ buffer: partBuf, index: i });
      console.log(`[ffmpeg] جزء ${i}: ${(partBuf.length/1024/1024).toFixed(2)}MB`);
      i++;
    }

    if (parts.length === 0) throw new Error('ffmpeg لم ينتج أي أجزاء');
    return parts;

  } finally {
    // تنظيف الملفات المؤقتة دائماً
    try {
      const fs2 = require('fs');
      fs2.rmSync(tmpDir, { recursive: true, force: true });
    } catch(_) {}
  }
}

// ── /tg-upload-chunk: استقبال الفيديو مجزّأً من التطبيق ──────
app.post('/tg-upload-chunk', (req, res) => {
  const secret = req.query.secret;
  if (secret !== PROXY_SECRET) return res.status(403).json({ success: false, error: 'forbidden' });

  const uploadId = req.query.uploadId;
  const isFinal  = req.query.final    === 'true';
  const isProbe  = req.query.probe    === 'true';
  const mime     = req.query.mime     || 'video/mp4';
  const fname    = req.query.filename || ('video_' + Date.now() + '.mp4');
  const chunkIdx = parseInt(req.query.idx || '0', 10);

  if (!uploadId) return res.status(400).json({ success: false, error: 'uploadId مطلوب' });

  if (!_uploadSessions.has(uploadId)) {
    _uploadSessions.set(uploadId, { chunks: [], mime, fname, lastSeen: Date.now() });
  }
  const session = _uploadSessions.get(uploadId);
  session.lastSeen = Date.now();

  const parts = [];
  req.on('data',  d => parts.push(d));
  req.on('error', e => { if (!res.headersSent) res.json({ success: false, error: e.message }); });

  req.on('end', async () => {
    const chunkBuf = Buffer.concat(parts);
    if (chunkBuf.length > 0) session.chunks[chunkIdx] = chunkBuf;

    const totalReceived = session.chunks.reduce((s, c) => s + (c ? c.length : 0), 0);
    console.log(`[chunk-upload] ${uploadId} idx=${chunkIdx} ${(chunkBuf.length/1024).toFixed(1)}KB probe=${isProbe} final=${isFinal} total=${(totalReceived/1024/1024).toFixed(2)}MB`);

    if (isProbe || !isFinal) {
      return res.json({ success: true, received: chunkBuf.length, total: totalReceived });
    }

    // ── آخر chunk: نجمع ونقسّم بـ ffmpeg (أجزاء MP4 سليمة) ──
    try {
      const maxIdx = session.chunks.reduce((m, _, i) => Math.max(m, i), 0);
      for (let i = 0; i <= maxIdx; i++) {
        if (!session.chunks[i]) session.chunks[i] = Buffer.alloc(0);
      }
      const fileBuffer = Buffer.concat(session.chunks.filter(Boolean));
      const totalSize  = fileBuffer.length;
      _uploadSessions.delete(uploadId);

      console.log(`[chunk-upload] ${uploadId} ${(totalSize/1024/1024).toFixed(2)}MB — تقسيم بـ ffmpeg`);

      // تقسيم صحيح بـ ffmpeg — كل جزء أقل من 45MB وملف MP4 سليم
      // لا MTProto نهائياً — Bot API فقط = لا خطر حظر
      const tgParts = totalSize <= 45 * 1024 * 1024
        ? [{ buffer: fileBuffer, index: 0 }]
        : await splitVideoFFmpeg(fileBuffer, fname, 45);

      const partIds = [];
      for (const part of tgParts) {
        const result = await uploadPartBotApi(part.buffer, fname, mime, part.index);
        partIds.push(result.file_id);
        console.log(`[chunk-upload] ✅ جزء ${part.index + 1}/${tgParts.length} → ${result.file_id.slice(0,15)}...`);
      }

      const token     = partIds.join('|');
      const streamUrl = `https://mahmoud08808665888888m-my-bot.hf.space/tg-stream/${encodeURIComponent(token)}`;
      console.log(`[chunk-upload] ✅ ${uploadId} اكتمل — ${partIds.length} جزء عبر Bot API فقط`);
      res.json({ success: true, file_id: token, url: streamUrl, type: 'video' });

    } catch (e) {
      console.error(`[chunk-upload] خطأ:`, e.message);
      _uploadSessions.delete(uploadId);
      if (!res.headersSent) res.json({ success: false, error: e.message });
    }
  });
});

// ── رفع الفيديو المباشر (تيار واحد) — نفس منطق chunk-upload ──
app.post('/tg-upload-video', (req, res) => {
  const secret = req.query.secret;
  if (secret !== PROXY_SECRET) return res.status(403).json({ success: false, error: 'forbidden' });

  const mime  = req.query.mime     || 'video/mp4';
  const fname = req.query.filename || ('video_' + Date.now() + '.mp4');

  const chunks = [];
  req.on('data',  chunk => chunks.push(chunk));
  req.on('error', err   => { if (!res.headersSent) res.json({ success: false, error: err.message }); });

  req.on('end', async () => {
    try {
      const fileBuffer = Buffer.concat(chunks);
      const totalSize  = fileBuffer.length;
      const totalMB    = (totalSize / 1024 / 1024).toFixed(2);

      console.log(`[tg-upload-video] ${totalMB}MB — تقسيم بـ ffmpeg`);

      // تقسيم صحيح بـ ffmpeg — Bot API فقط لا MTProto
      const tgParts = totalSize <= 45 * 1024 * 1024
        ? [{ buffer: fileBuffer, index: 0 }]
        : await splitVideoFFmpeg(fileBuffer, fname, 45);

      const partIds = [];
      for (const part of tgParts) {
        const result = await uploadPartBotApi(part.buffer, fname, mime, part.index);
        partIds.push(result.file_id);
        console.log(`[tg-upload-video] ✅ جزء ${part.index + 1}/${tgParts.length} → ${result.file_id.slice(0,15)}...`);
      }

      const token     = partIds.join('|');
      const streamUrl = `https://mahmoud08808665888888m-my-bot.hf.space/tg-stream/${encodeURIComponent(token)}`;
      res.json({ success: true, file_id: token, url: streamUrl, type: 'video' });

    } catch (e) {
      console.error('[tg-upload-video] خطأ:', e.message);
      if (!res.headersSent) res.json({ success: false, error: e.message });
    }
  });
});

// ═══ تيليغرام: رفع ملف (صورة / فيديو / PDF) ═════════════════
// ══════════════════════════════════════════════════════════════
// يستقبل الملف كـ base64 ويرفعه لقناة التخزين الخاصة
// يعيد: { success, file_id, url, type }
app.post('/tg-upload', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { base64, mime, filename } = req.body;
  if (!base64 || !mime) return res.json({ success: false, error: 'base64 و mime مطلوبان' });

  try {
    const buffer = Buffer.from(base64, 'base64');
    const boundary = '----TGUpload' + Date.now().toString(36);
    const CRLF = '\r\n';

    // تحديد نوع الإرسال
    const isPhoto    = mime.startsWith('image/') && !mime.includes('gif');
    const isVideo    = mime.startsWith('video/');
    const isPDF      = mime === 'application/pdf';
    const isAudio    = mime.startsWith('audio/');
    let endpoint, fieldName;
    if (isPhoto)       { endpoint = '/sendPhoto';    fieldName = 'photo'; }
    else if (isVideo)  { endpoint = '/sendVideo';    fieldName = 'video'; }
    else if (isAudio)  { endpoint = '/sendAudio';    fieldName = 'audio'; }
    else               { endpoint = '/sendDocument'; fieldName = 'document'; }

    const fname = filename || ('file_' + Date.now() + (isPhoto ? '.jpg' : isVideo ? '.mp4' : isPDF ? '.pdf' : '.bin'));

    let headerStr = '';
    const encodeField = (name, value) =>
      `--${boundary}${CRLF}Content-Disposition: form-data; name="${name}"${CRLF}${CRLF}${value}${CRLF}`;
    headerStr += encodeField('chat_id', STORAGE_CHANNEL_ID);
    headerStr += `--${boundary}${CRLF}Content-Disposition: form-data; name="${fieldName}"; filename="${fname}"${CRLF}Content-Type: ${mime}${CRLF}${CRLF}`;
    const footer = `${CRLF}--${boundary}--${CRLF}`;
    const body = Buffer.concat([Buffer.from(headerStr, 'utf-8'), buffer, Buffer.from(footer, 'utf-8')]);

    const r = await fetch(STORAGE_BOT_API + endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': `multipart/form-data; boundary=${boundary}`,
        'Content-Length': body.length,
      },
      body,
    });
    const data = await r.json();
    if (!data.ok) return res.json({ success: false, error: data.description || 'فشل الرفع' });

    // استخرج file_id حسب النوع
    const msg = data.result;
    let file_id = null;
    let type = 'document';
    if (msg.photo)    { file_id = msg.photo[msg.photo.length - 1].file_id; type = 'image'; }
    else if (msg.video)    { file_id = msg.video.file_id;    type = 'video'; }
    else if (msg.audio)    { file_id = msg.audio.file_id;    type = 'audio'; }
    else if (msg.document) { file_id = msg.document.file_id; type = isPDF ? 'pdf' : 'document'; }

    if (!file_id) return res.json({ success: false, error: 'لم يُعثر على file_id' });

    // بناء رابط البث
    const proxyBase = 'https://mahmoud08808665888888m-my-bot.hf.space';
    const streamUrl = `${proxyBase}/tg-stream/${encodeURIComponent(file_id)}`;

    res.json({ success: true, file_id, url: streamUrl, type });
  } catch (e) {
    console.error('[tg-upload] خطأ:', e.message);
    res.json({ success: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
// ═══ تيليغرام: بث الملف (Streaming مع Range Requests) ═══════
// ══════════════════════════════════════════════════════════════
// مفتوح للعموم (بدون secret) لأن المتصفح/WebView يطلبه مباشرة
// الحماية: file_id لا يُعرف إلا لمن رفع الملف (يُخزن في Firestore)
// ─── helper: تخمين MIME من امتداد الملف ───────────────────────
function guessMimeFromPath(filePath) {
  if (!filePath) return null;
  const ext = filePath.split('.').pop().toLowerCase();
  const map = {
    'mp4':'video/mp4', 'mov':'video/quicktime', 'avi':'video/x-msvideo',
    'mkv':'video/x-matroska', 'webm':'video/webm', 'flv':'video/x-flv',
    '3gp':'video/3gpp', 'wmv':'video/x-ms-wmv', 'm4v':'video/mp4',
    'jpg':'image/jpeg', 'jpeg':'image/jpeg', 'png':'image/png',
    'gif':'image/gif', 'webp':'image/webp', 'heic':'image/heic',
    'heif':'image/heif', 'bmp':'image/bmp',
    'pdf':'application/pdf', 'mp3':'audio/mpeg', 'ogg':'audio/ogg',
    'm4a':'audio/mp4', 'aac':'audio/aac', 'wav':'audio/wav',
  };
  return map[ext] || null;
}

// ══ tg-stream: جلب الفيديو عبر Bot API بـ Range requests ══
// token = file_id واحد (ملف صغير) أو file_id1|file_id2|... (أجزاء 15MB)
// الجلب دائماً عبر Bot API — بدون MTProto نهائياً

app.get('/tg-stream/:token', async (req, res) => {
  const raw = decodeURIComponent(req.params.token);
  if (!raw) return res.status(400).send('token مطلوب');

  const CACHE_TTL = 50 * 60 * 1000; // 50 دقيقة

  // ── helper: جلب file_path + size مع كاش ──
  async function resolvePart(file_id) {
    const ck     = 'part:' + file_id;
    const cached = fileUrlCache.get(ck);
    if (cached && cached.expiry > Date.now()) return cached;
    const r    = await fetch(`${STORAGE_BOT_API}/getFile?file_id=${encodeURIComponent(file_id)}`);
    const info = await r.json();
    if (!info.ok || !info.result?.file_path)
      throw new Error('getFile فشل: ' + file_id.slice(0, 15));
    const entry = {
      filePath: info.result.file_path,
      size:     info.result.file_size || 0,
      expiry:   Date.now() + CACHE_TTL,
    };
    fileUrlCache.set(ck, entry);
    return entry;
  }

  try {
    const partIds   = raw.split('|');
    const partInfos = await Promise.all(partIds.map(resolvePart));
    const totalSize = partInfos.reduce((s, p) => s + p.size, 0);
    const mime      = guessMimeFromPath(partInfos[0].filePath) || 'video/mp4';

    // ── Range request ──
    const rangeHeader = req.headers.range;
    let start = 0, end = totalSize - 1;
    if (rangeHeader) {
      const m = rangeHeader.match(/bytes=(\d+)-(\d*)/);
      if (m) {
        start = parseInt(m[1], 10);
        end   = m[2] ? parseInt(m[2], 10) : totalSize - 1;
      }
    }
    end = Math.min(end, totalSize - 1);

    res.writeHead(rangeHeader ? 206 : 200, {
      'Content-Type':   mime,
      'Content-Length': end - start + 1,
      'Content-Range':  `bytes ${start}-${end}/${totalSize}`,
      'Accept-Ranges':  'bytes',
      'Cache-Control':  'no-cache',
    });

    // ── Pipe مباشر: تيليغرام → المستخدم بدون تخزين في RAM ──
    let byteOffset = 0;
    for (let p = 0; p < partInfos.length && !res.destroyed; p++) {
      const partStart = byteOffset;
      const partEnd   = byteOffset + partInfos[p].size - 1;
      byteOffset     += partInfos[p].size;

      if (partEnd < start || partStart > end) continue;

      const localStart = Math.max(0, start - partStart);
      const localEnd   = Math.min(partInfos[p].size - 1, end - partStart);
      const tgUrl      = `https://api.telegram.org/file/bot${STORAGE_BOT_TOKEN}/${partInfos[p].filePath}`;

      const tgRes = await fetch(tgUrl, { headers: { Range: `bytes=${localStart}-${localEnd}` } });
      if (!tgRes.ok && tgRes.status !== 206)
        throw new Error(`تيليغرام: ${tgRes.status} للجزء ${p + 1}`);

      // pipe مباشر chunk-by-chunk — صفر RAM
      // node-fetch يُعيد PassThrough (Node stream) — نستخدم .on() مباشرة بدون fromWeb
      await new Promise((resolve, reject) => {
        const src = tgRes.body;
        src.on('data',  chunk => { if (!res.destroyed) res.write(chunk); });
        src.on('end',   resolve);
        src.on('error', reject);
        res.on('close', () => { try { src.destroy(); } catch(_){} resolve(); });
      });
    }

    if (!res.destroyed) res.end();

  } catch (e) {
    console.error('[tg-stream] خطأ:', e.message);
    if (!res.headersSent) res.status(500).json({ error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
// ═══ تيليغرام: جلب بيانات التنزيل (مخفي عن التطبيق) ═════════
// ══════════════════════════════════════════════════════════════
// يُعيد workerUrl + tgPath للتطبيق فقط — رابط Cloudflare مخفي هنا
// التطبيق يبني: workerUrl + tgPath ويبدأ التنزيل المباشر عبر Cloudflare
const CF_WORKER_URL = 'https://caro-tg-stream.hafezalmahmoud095.workers.dev';

// ── tgFetch: كل طلبات api.telegram.org تمر عبر Cloudflare Worker ──
// HuggingFace يحجب الاتصال المباشر بتيليغرام — Worker يتجاوز الحجب
async function tgFetch(url, options = {}) {
  // نحوّل https://api.telegram.org/... إلى https://worker/?url=...
  const proxied = CF_WORKER_URL + '/?url=' + encodeURIComponent(url);
  return fetch(proxied, options);
}

// tg-download: يُعيد streamUrl مناسب حسب عدد الأجزاء
// جزء واحد  → /tg-stream مباشرة (pipe سريع)
// أجزاء متعددة → /tg-merge  (دمج بـ ffmpeg → ملف واحد سليم)
app.get('/tg-download/:token', async (req, res) => {
  const raw = decodeURIComponent(req.params.token);
  if (!raw) return res.status(400).json({ error: 'token مطلوب' });
  res.setHeader('Access-Control-Allow-Origin', '*');
  const proxyBase = process.env.PROXY_BASE_URL || 'https://mahmoud08808665888888m-my-bot.hf.space';
  const parts     = raw.split('|');
  if (parts.length === 1) {
    // جزء واحد → stream مباشر
    res.json({ streamUrl: `${proxyBase}/tg-stream/${encodeURIComponent(raw)}` });
  } else {
    // أجزاء متعددة → دمج بـ ffmpeg أولاً
    res.json({ streamUrl: `${proxyBase}/tg-merge/${encodeURIComponent(raw)}` });
  }
});

// ── tg-merge: يجلب أجزاء الفيديو من تيليغرام ويدمجها بـ ffmpeg ──
// يُعيد ملف MP4 واحد سليم كامل للتنزيل
app.get('/tg-merge/:token', async (req, res) => {
  const raw = decodeURIComponent(req.params.token);
  if (!raw) return res.status(400).send('token مطلوب');

  const os   = require('os');
  const path = require('path');
  const fs   = require('fs');
  const { execSync } = require('child_process');
  const CACHE_TTL = 50 * 60 * 1000;

  async function resolvePartMerge(file_id) {
    const ck     = 'part:' + file_id;
    const cached = fileUrlCache.get(ck);
    if (cached && cached.expiry > Date.now()) return cached;
    const r    = await fetch(`${STORAGE_BOT_API}/getFile?file_id=${encodeURIComponent(file_id)}`);
    const info = await r.json();
    if (!info.ok || !info.result?.file_path)
      throw new Error('getFile فشل: ' + file_id.slice(0, 15));
    const entry = { filePath: info.result.file_path, size: info.result.file_size || 0, expiry: Date.now() + CACHE_TTL };
    fileUrlCache.set(ck, entry);
    return entry;
  }

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tgmerge_'));
  try {
    const partIds   = raw.split('|');
    const partInfos = await Promise.all(partIds.map(resolvePartMerge));

    console.log(`[tg-merge] ${partIds.length} جزء — جلب ودمج...`);

    // ① جلب كل جزء من تيليغرام وحفظه مؤقتاً
    const partPaths = [];
    for (let i = 0; i < partInfos.length; i++) {
      const tgUrl   = `https://api.telegram.org/file/bot${STORAGE_BOT_TOKEN}/${partInfos[i].filePath}`;
      const tgRes   = await fetch(tgUrl);
      if (!tgRes.ok) throw new Error(`تيليغرام: ${tgRes.status} للجزء ${i + 1}`);
      const buf     = Buffer.from(await tgRes.arrayBuffer());
      const partPath = path.join(tmpDir, `part_${i}.mp4`);
      fs.writeFileSync(partPath, buf);
      partPaths.push(partPath);
      console.log(`[tg-merge] ✅ جزء ${i + 1}/${partInfos.length} (${(buf.length/1024/1024).toFixed(2)}MB)`);
    }

    // ② دمج بـ ffmpeg concat
    const listPath  = path.join(tmpDir, 'list.txt');
    const outPath   = path.join(tmpDir, 'merged.mp4');
    const listContent = partPaths.map(p => `file '${p}'`).join('\n');
    fs.writeFileSync(listPath, listContent);

    execSync(
      `ffmpeg -f concat -safe 0 -i "${listPath}" -c copy -movflags +faststart "${outPath}" -y`,
      { timeout: 300000 }
    );

    const merged   = fs.readFileSync(outPath);
    const fname    = 'video_' + Date.now() + '.mp4';

    console.log(`[tg-merge] ✅ دمج مكتمل — ${(merged.length/1024/1024).toFixed(2)}MB`);

    // ③ إرسال الملف المدموج للمستخدم
    res.writeHead(200, {
      'Content-Type':        'video/mp4',
      'Content-Length':      merged.length,
      'Content-Disposition': `attachment; filename="${fname}"`,
      'Cache-Control':       'no-cache',
    });
    res.end(merged);

  } catch (e) {
    console.error('[tg-merge] خطأ:', e.message);
    if (!res.headersSent) res.status(500).json({ error: e.message });
  } finally {
    // تنظيف الملفات المؤقتة دائماً
    try { require('fs').rmSync(tmpDir, { recursive: true, force: true }); } catch(_) {}
  }
});

// ══════════════════════════════════════════════════════════════
// ═══ تيليغرام: حذف رسالة (الملف) من القناة ══════════════════
// ══════════════════════════════════════════════════════════════
// يستقبل message_id ويحذف الرسالة من قناة التخزين
app.post('/tg-delete', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { message_id } = req.body;
  if (!message_id) return res.json({ success: false, error: 'message_id مطلوب' });
  try {
    const r = await fetch(STORAGE_BOT_API + '/deleteMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: STORAGE_CHANNEL_ID, message_id })
    });
    const data = await r.json();
    res.json({ success: data.ok, result: data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

app.post('/send-email', async (req, res) => {
  const { to_email, otp_code, user_name } = req.body;
  try {
    const emailjs = require('@emailjs/nodejs');
    await emailjs.send('service_cb3prgt', 'template_q3by3k4', {
      email: to_email,
      otp_code: otp_code,
      user_name: user_name || 'مستخدم'
    }, { publicKey: 'ZItSgkAWpyo2cTF5I', privateKey: 'OFjz4oBlbmnFWgCc0DJiu' });
    res.json({ success: true });
  } catch (err) {
    console.error('EmailJS error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/send-sms', async (req, res) => {
  const { phone, message } = req.body;
  try {
    const smsDoc = await new Promise((resolve, reject) => {
      const options = {
        hostname: 'firestore.googleapis.com',
        path: '/v1/projects/' + FIREBASE_PROJECT + '/databases/(default)/documents/settings/smsGateway?key=' + FIREBASE_API_KEY,
        method: 'GET'
      };
      const r = https.request(options, (res) => {
        let data = '';
        res.on('data', d => data += d);
        res.on('end', () => resolve(JSON.parse(data)));
      });
      r.on('error', reject);
      r.end();
    });
    const smsUrl = smsDoc.fields.url.stringValue;
    const smsResp = await fetch(smsUrl + '/send-sms', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ phone, message, secret: 'caro_secret_2025_xK9#mQ' })
    });
    const data = await smsResp.json();
    res.json(data);
  } catch (err) {
    console.error('SMS proxy error:', err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ══════════════════════════════════════════════
// ═══ تيليغرام: webhook يستقبل رسائل البوت ════
// ══════════════════════════════════════════════
app.post('/telegram-webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const update = req.body;
    const msg = update && update.message;
    if (!msg) return;

    const chatId = msg.chat.id.toString();
    const ALLOWED_CHAT_ID = '6245764342';
    if (chatId !== ALLOWED_CHAT_ID) return;
    const text = (msg.text || '').trim();
    const firstName = msg.chat.first_name || 'مستخدم';
    const username = msg.chat.username ? '@' + msg.chat.username : 'لا يوجد';

    // ─── أولوية 1: contact (زر مشاركة رقم هاتفي) ─────────────────
    // يجب معالجته أولاً لأن تلغرام قد يرسل msg.text مع msg.contact
    if (msg.contact) {
      const contact = msg.contact;
      // التحقق الصارم: user_id يجب موجوداً ومطابقاً لـ chatId
      // user_id فارغ = جهة اتصال من دفتر الأرقام → مرفوض
      const contactUserId = contact.user_id ? contact.user_id.toString() : null;
      if (!contactUserId || contactUserId !== chatId) {
        await fetch(TELEGRAM_API + '/sendMessage', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: '❌ يجب الضغط على زر "📱 مشاركة رقم هاتفي" الخاص بك مباشرةً.\n\nلا يُقبل إرسال رقم شخص آخر.',
            reply_markup: {
              keyboard: [[{ text: '📱 مشاركة رقم هاتفي', request_contact: true }]],
              resize_keyboard: true,
              one_time_keyboard: true,
              is_persistent: true
            }
          })
        });
        const { createHash } = require('crypto');
        const maskedLog = createHash('sha256').update(chatId + (contact.phone_number || '') + Date.now()).digest('hex').slice(0, 16);
        console.log('[verify] blocked_attempt ref=' + maskedLog);
        return;
      }
      // ✅ contact صحيح، يكمل أدناه
    }

    // ─── أولوية 2: /start ──────────────────────────────────────────
    if (!msg.contact && (text === '/start' || text.startsWith('/start'))) {
      await fetch(TELEGRAM_API + '/sendMessage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: '👋 أهلاً بك في بوت كارو!\n\n🔐 لاستلام كود التحقق، اضغط الزر ⬇️ أدناه مباشرة لمشاركة رقم هاتفك.',
          reply_markup: {
            keyboard: [[{
              text: '📱 مشاركة رقم هاتفي',
              request_contact: true
            }]],
            resize_keyboard: true,
            one_time_keyboard: true,
            is_persistent: true
          }
        })
      });
      return;
    }

    // ─── أولوية 3: رفض أي نص عادي (ليس contact ولا /start) ───────
    if (!msg.contact && msg.text) {
      await fetch(TELEGRAM_API + '/sendMessage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: '⚠️ يرجى الضغط على زر "📱 مشاركة رقم هاتفي" الموجود بالأسفل فقط.',
          reply_markup: {
            keyboard: [[{ text: '📱 مشاركة رقم هاتفي', request_contact: true }]],
            resize_keyboard: true,
            one_time_keyboard: true,
            is_persistent: true
          }
        })
      });
      return;
    }

    // ─── إذا لم يكن contact → تجاهل تام ──────────────────────────
    if (!msg.contact) return;

    const contact = msg.contact;

    let phone = contact.phone_number;
    if (!phone.startsWith('+')) phone = '+' + phone;

    // ─── التحقق أن التطبيق طلب الكود فعلاً ─────────────────────
    // نستخدم fetch2 منفصل لتجنب أي تعارض مع fetch الخارجي
    const fetch2 = _nodeFetch;  // عبر CF proxy
    const docIdP = phone.replace('+', '');
    let pendingOk = false;
    let pendingExpiry = 0;
    try {
      const pSnap = await fetch2(
        'https://firestore.googleapis.com/v1/projects/' + FIREBASE_PROJECT +
        '/databases/(default)/documents/telegramPending/' + docIdP + '?key=' + FIREBASE_API_KEY
      );
      const pData = await pSnap.json().catch(() => null);
      if (pData && pData.fields && !pData.error) {
        const pExpiry = Number(pData.fields.expiry?.integerValue || 0);
        const pUsed   = pData.fields.used?.booleanValue === true;
        if (!pUsed && pExpiry > Date.now()) {
          pendingOk     = true;
          pendingExpiry = pExpiry;
        }
      }
    } catch(e) { console.error('[pending check]', e.message); }

    if (!pendingOk) {
      await fetch2(TELEGRAM_API + '/sendMessage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: '⚠️ لم يتم طلب كود تحقق من التطبيق.\n\nافتح تطبيق كارو أولاً واختر التحقق عبر تيليغرام، ثم عد هنا.',
          reply_markup: { remove_keyboard: true }
        })
      });
      return;
    }

    // throttle: منع طلب كود جديد قبل انتهاء الكود السابق
    const existing = otpStore[phone];
    if (existing && (existing.expiry - Date.now()) > 0) {
      const remainSec = Math.ceil((existing.expiry - Date.now()) / 1000);
      const remainMin = Math.floor(remainSec / 60);
      const remainS   = remainSec % 60;
      const timeStr   = remainMin + ':' + (remainS < 10 ? '0' : '') + remainS;

      // فحص هل الكود السابق لا يزال غير مُستخدم من Firestore
      let codeUsed = false;
      try {
        const verSnap = await fetch2(
          'https://firestore.googleapis.com/v1/projects/' + FIREBASE_PROJECT +
          '/databases/(default)/documents/telegramVerify/' + docIdP + '?key=' + FIREBASE_API_KEY
        );
        const verData = await verSnap.json().catch(() => null);
        if (verData && verData.fields) {
          codeUsed = verData.fields.used?.booleanValue === true;
        }
      } catch(e) { /* نتجاهل الخطأ */ }

      let throttleMsg = '';
      if (!codeUsed) {
        throttleMsg =
          '⚠️ لديك كود تحقق لم تُدخله بعد!\n\n' +
          '📌 ارجع إلى تطبيق كارو وأدخل الكود الذي أُرسل إليك.\n\n' +
          '⏱ الوقت المتبقي للكود الحالي: *' + timeStr + '* دقيقة\n\n' +
          'يمكنك طلب كود جديد بعد انتهاء هذا الوقت.';
      } else {
        throttleMsg =
          '⏳ تم إرسال كود مسبقاً.\n\n' +
          '⏱ يمكنك طلب كود جديد بعد *' + timeStr + '* دقيقة.';
      }

      await fetch2(TELEGRAM_API + '/sendMessage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          parse_mode: 'Markdown',
          text: throttleMsg,
          reply_markup: { remove_keyboard: true }
        })
      });
      return;
    }

    // التحقق أن الرقم مسجل في كارو
    let caroName = firstName;
    let caroUsername = username;
    let phoneFoundInCaro = false;
    try {
      const usersSnap = await fetch2(
        'https://firestore.googleapis.com/v1/projects/' + FIREBASE_PROJECT + '/databases/(default)/documents:runQuery?key=' + FIREBASE_API_KEY,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            structuredQuery: {
              from: [{ collectionId: 'users' }],
              where: {
                fieldFilter: {
                  field: { fieldPath: 'phone' },
                  op: 'EQUAL',
                  value: { stringValue: phone }
                }
              },
              limit: 1
            }
          })
        }
      );
      const usersData = await usersSnap.json();
      const userDoc = usersData && usersData[0] && usersData[0].document;
      if (userDoc && userDoc.fields) {
        phoneFoundInCaro = true;
        const f = userDoc.fields;
        if (f.fullName && f.fullName.stringValue) caroName = f.fullName.stringValue;
        if (f.username && f.username.stringValue) caroUsername = '@' + f.username.stringValue;
      }
    } catch(e) {
      console.error('Firestore lookup error:', e.message);
    }

    // الرقم غير مسجل في كارو
    if (!phoneFoundInCaro) {
      await fetch2(TELEGRAM_API + '/sendMessage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: '❌ رقم هاتفك (' + phone + ') غير مسجل في تطبيق كارو.\n\nتأكد من أنك سجّلت بنفس الرقم في التطبيق.',
          reply_markup: { remove_keyboard: true }
        })
      });
      return;
    }

    const code = Math.floor(100000 + Math.random() * 900000).toString();
    const expiry = Date.now() + 5 * 60 * 1000;

    // حفظ مؤقت في الذاكرة
    otpStore[phone] = { code, chatId, expiry };

    // حفظ في Firestore
    const docId = phone.replace('+', '');
    await fetch2('https://firestore.googleapis.com/v1/projects/' + FIREBASE_PROJECT + '/databases/(default)/documents/telegramVerify/' + docId + '?key=' + FIREBASE_API_KEY, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        fields: {
          chatId: { stringValue: chatId },
          phone: { stringValue: phone },
          code: { stringValue: code },
          expiry: { integerValue: expiry.toString() },
          createdAt: { integerValue: Date.now().toString() }
        }
      })
    });

    // ─── تعليم الطلب المعلق كـ used لمنع إعادة الاستخدام ──────
    try {
      await fetch2(
        'https://firestore.googleapis.com/v1/projects/' + FIREBASE_PROJECT +
        '/databases/(default)/documents/telegramPending/' + docId + '?key=' + FIREBASE_API_KEY,
        {
          method: 'PATCH',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            fields: {
              phone:     { stringValue: phone },
              expiry:    { integerValue: pendingExpiry.toString() },
              createdAt: { integerValue: Date.now().toString() },
              used:      { booleanValue: true }
            }
          })
        }
      );
    } catch(e) { console.error('[mark used]', e.message); }

    // حساب الوقت المتبقي للكود
    const otpRemainSec = Math.ceil((expiry - Date.now()) / 1000);
    const otpRemainMin = Math.floor(otpRemainSec / 60);
    const otpRemainS   = otpRemainSec % 60;
    const otpTimeStr   = otpRemainMin + ':' + (otpRemainS < 10 ? '0' : '') + otpRemainS;

    // رسالة الترحيب مع المؤقت + إزالة الكيبورد
    await fetch2(TELEGRAM_API + '/sendMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        parse_mode: 'Markdown',
        text:
          '🎉 أهلاً وسهلاً *' + caroName + '*!\n\n' +
          '👤 الاسم: *' + caroName + '*\n' +
          '🔗 المعرّف: *' + caroUsername + '*\n\n' +
          '⏱ صالح لمدة *' + otpTimeStr + '* دقيقة\n\n' +
          '🔐 كود التحقق الخاص بك في كارو:',
        reply_markup: { remove_keyboard: true }
      })
    });

    // الكود وحده في رسالة منفصلة لتسهيل النسخ
    await fetch2(TELEGRAM_API + '/sendMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: code
      })
    });

  } catch (e) {
    console.error('Telegram webhook error:', e.message);
  }
});

// ══════════════════════════════════════════════
// ═══ التحقق من كود تيليغرام (يستدعيه كارو) ══
// ══════════════════════════════════════════════
// ══════════════════════════════════════════════════════════
// ═══ طلب كود تلغرام من التطبيق (يجب أن يُستدعى أولاً) ════
// ══════════════════════════════════════════════════════════
app.post('/request-telegram-otp', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { phone } = req.body;
  if (!phone) return res.json({ success: false, error: 'رقم الهاتف مطلوب' });

  const normalPhone = phone.startsWith('+') ? phone : '+' + phone;
  const docId = normalPhone.replace('+', '');
  const expiry = Date.now() + 5 * 60 * 1000; // 5 دقائق

  try {
    // حفظ الطلب المعلق في Firestore
    await fetch(
      'https://firestore.googleapis.com/v1/projects/' + FIREBASE_PROJECT +
      '/databases/(default)/documents/telegramPending/' + docId + '?key=' + FIREBASE_API_KEY,
      {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          fields: {
            phone:     { stringValue: normalPhone },
            expiry:    { integerValue: expiry.toString() },
            createdAt: { integerValue: Date.now().toString() },
            used:      { booleanValue: false }
          }
        })
      }
    );
    res.json({ success: true, expiry });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

app.post('/verify-telegram-otp', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { phone, code } = req.body;
  if (!phone || !code) return res.json({ success: false, error: 'بيانات ناقصة' });

  const normalPhone = phone.startsWith('+') ? phone : '+' + phone;
  const record = otpStore[normalPhone];

  if (!record) return res.json({ success: false, error: 'لم يتم إرسال كود لهذا الرقم، تأكد من إرسال رقمك للبوت أولاً' });
  if (Date.now() > record.expiry) {
    delete otpStore[normalPhone];
    return res.json({ success: false, error: 'انتهت صلاحية الكود، أرسل رقمك للبوت مجدداً' });
  }
  if (record.code !== code) return res.json({ success: false, error: 'الكود غير صحيح' });

  delete otpStore[normalPhone];
  res.json({ success: true, chatId: record.chatId });
});

app.post('/chat', async (req, res) => {
  const { messages, model, max_tokens, temperature } = req.body;
  if (!checkSecret(req, res)) return;
  try {
    const orRes = await fetch('https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + OPENROUTER_API_KEY,
        'HTTP-Referer': 'https://caroinsyria.web.app',
        'X-Title': 'Caro AI'
      },
      body: JSON.stringify({
        model: 'meta-llama/llama-3.3-70b-instruct:free',
        messages,
        max_tokens: max_tokens || 1024,
        temperature: temperature || 0.7
      })
    });
    const data = await orRes.json();
    res.json(data);
  } catch (err) {
    console.error('Gemini error:', err);
    res.status(500).json({ error: err.message });
  }
});


// ══════════════════════════════════════════════
// ═══ GitHub: جلب آخر إصدار (للمستخدمين) ══════
// ══════════════════════════════════════════════
app.get('/github-latest-release', async (req, res) => {
  try {
    const r = await fetch(
      'https://api.github.com/repos/' + GITHUB_OWNER + '/' + GITHUB_REPO + '/releases/latest',
      { headers: { 'Authorization': 'Bearer ' + GITHUB_TOKEN, 'Accept': 'application/vnd.github+json' } }
    );
    const data = await r.json();
    if (!data.tag_name) return res.json({ success: false, error: 'لا يوجد إصدار بعد' });
    const apk = (data.assets || []).find(a => a.name.endsWith('.apk'));
    res.json({
      success: true,
      version: data.tag_name.replace('v', ''),
      tag: data.tag_name,
      notes: data.body || '',
      download_url: apk ? apk.browser_download_url : null,
      published_at: data.published_at
    });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ══════════════════════════════════════════════
// ═══ GitHub: رفع إصدار جديد (للمدير فقط) ════
// ══════════════════════════════════════════════
app.post('/github-create-release', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { version, notes, apk_base64, apk_name } = req.body;
  if (!version || !apk_base64 || !apk_name)
    return res.status(400).json({ success: false, error: 'version و apk_base64 و apk_name مطلوبة' });
  try {
    const ghHeaders = {
      'Authorization': 'Bearer ' + GITHUB_TOKEN,
      'Accept': 'application/vnd.github+json',
      'Content-Type': 'application/json'
    };
    const tag = 'v' + version;

    // 1. حذف الـ release القديم إن وجد بنفس الـ tag
    const existingRes = await fetch(
      'https://api.github.com/repos/' + GITHUB_OWNER + '/' + GITHUB_REPO + '/releases/tags/' + tag,
      { headers: ghHeaders }
    );
    if (existingRes.ok) {
      const existing = await existingRes.json();
      if (existing.id) {
        // حذف الـ release
        await fetch(
          'https://api.github.com/repos/' + GITHUB_OWNER + '/' + GITHUB_REPO + '/releases/' + existing.id,
          { method: 'DELETE', headers: ghHeaders }
        );
        // حذف الـ tag
        await fetch(
          'https://api.github.com/repos/' + GITHUB_OWNER + '/' + GITHUB_REPO + '/git/refs/tags/' + tag,
          { method: 'DELETE', headers: ghHeaders }
        );
        console.log('[GitHub] حُذف الإصدار القديم:', tag);
      }
    }

    // 2. إنشاء Release جديد
    const releaseRes = await fetch(
      'https://api.github.com/repos/' + GITHUB_OWNER + '/' + GITHUB_REPO + '/releases',
      {
        method: 'POST',
        headers: ghHeaders,
        body: JSON.stringify({
          tag_name: tag,
          name: 'كارو ' + tag,
          body: notes || '',
          draft: false,
          prerelease: false
        })
      }
    );
    const release = await releaseRes.json();
    console.log('[GitHub] release response:', JSON.stringify(release).slice(0, 300));
    if (!release.upload_url)
      return res.status(500).json({ success: false, error: 'فشل إنشاء الإصدار', detail: release });

    // 3. رفع ملف APK
    const uploadUrl = release.upload_url.replace('{?name,label}', '') + '?name=' + encodeURIComponent(apk_name);
    const apkBuffer = Buffer.from(apk_base64, 'base64');
    const uploadRes = await fetch(uploadUrl, {
      method: 'POST',
      headers: {
        'Authorization': 'Bearer ' + GITHUB_TOKEN,
        'Content-Type': 'application/vnd.android.package-archive',
        'Content-Length': apkBuffer.length
      },
      body: apkBuffer
    });
    const asset = await uploadRes.json();
    console.log('[GitHub] asset response:', JSON.stringify(asset).slice(0, 200));
    if (!asset.browser_download_url)
      return res.status(500).json({ success: false, error: 'فشل رفع ملف APK', detail: asset });

    res.json({
      success: true,
      release_url: release.html_url,
      download_url: asset.browser_download_url,
      version: version
    });
  } catch (err) {
    console.error('[GitHub] خطأ:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});


// endpoint لتنزيل APK من GitHub وتمريره للعميل (لتجنب مشكلة redirect في WebView)
app.get('/github-download-apk', async (req, res) => {
  const url = req.query.url;
  if (!url || !url.startsWith('https://github.com')) {
    return res.status(400).json({ success: false, error: 'رابط غير صالح' });
  }
  try {
    const response = await fetch(url, {
      headers: {
        'Authorization': 'Bearer ' + GITHUB_TOKEN,
        'Accept': 'application/octet-stream'
      },
      redirect: 'follow'
    });
    if (!response.ok) {
      return res.status(response.status).json({ success: false, error: 'فشل جلب الملف من GitHub' });
    }
    res.setHeader('Content-Type', 'application/vnd.android.package-archive');
    res.setHeader('Content-Disposition', 'attachment; filename="Caro-Update.apk"');
    const contentLength = response.headers.get('content-length');
    if (contentLength) res.setHeader('Content-Length', contentLength);
    response.body.pipe(res);
    response.body.on('error', (err) => {
      console.error('[download-apk] pipe error:', err.message);
      if (!res.headersSent) res.status(500).send('خطأ في التنزيل');
      else res.destroy();
    });
    res.on('close', () => { try { response.body.destroy(); } catch(_) {} });
  } catch (err) {
    console.error('[download-apk] خطأ:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get('/register-services-webhook', async (req, res) => {
  try {
    const webhookUrl = 'https://mahmoud08808665888888m-my-bot.hf.space/services-bot-webhook';
    const r = await fetch(SERVICES_BOT_API + '/setWebhook', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: webhookUrl })
    });
    const data = await r.json();
    await reqSetBotStartCommand(SERVICES_BOT_API);
    res.json({ success: data.ok, result: data });
  } catch(e) {
    res.json({ success: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════
// ═══ بوت خدمات كارو: webhook يستقبل رسائل المستخدمين ═
// ══════════════════════════════════════════════════════
app.post('/services-bot-webhook', (req, res) => {
  // نرد على تيليغرام فوراً بـ 200 لمنع إعادة الإرسال
  res.sendStatus(200);

  // نُشغّل المنطق بشكل غير متزامن بعد إغلاق الـ response
  setImmediate(async () => {
    try {
      const update = req.body;

      const msg = update && update.message;
      if (!msg) return;

      const chatId    = msg.chat.id.toString();
      const ALLOWED_CHAT_ID = '6245764342';
      if (chatId !== ALLOWED_CHAT_ID) return;
      const text      = (msg.text || '').trim();
      const firstName = msg.chat.first_name || 'مستخدم';
      const username  = msg.chat.username ? '@' + msg.chat.username : '';

      // تخزين الرسالة في الذاكرة ليطّلع عليها المدير
      serviceMessages.push({
        id: Date.now().toString(),
        chatId,
        firstName,
        username,
        text,
        date: new Date().toISOString(),
        read: false
      });
      if (serviceMessages.length > 500) serviceMessages.shift();

      // ─── /start ───────────────────────────────────────────────
      if (text === '/start' || text.startsWith('/start ')) {
        // مسح الجلسة والمكدس عند /start دائماً
        delete aiSessions[chatId];
        userNavStack[chatId] = [];
        const cfg = servicesBotConfig;
        const reply_markup = buildReplyKeyboard(cfg.buttons, true); // true = أضف زر APK
        if ((cfg.attachType === 'image' || cfg.attachType === 'pdf') && cfg.attachUrl) {
          const isPhoto  = cfg.attachType === 'image';
          const endpoint = isPhoto ? '/sendPhoto' : '/sendDocument';
          const mediaKey = isPhoto ? 'photo'     : 'document';
          const body2 = { chat_id: chatId, [mediaKey]: cfg.attachUrl, caption: cfg.welcomeText };
          if (reply_markup) body2.reply_markup = reply_markup;
          await fetch(SERVICES_BOT_API + endpoint, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body2)
          });
        } else {
          const msgBody = { chat_id: chatId, text: cfg.welcomeText };
          if (reply_markup) msgBody.reply_markup = reply_markup;
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(msgBody)
          });
        }
        return;
      }

      // ─── زر تنزيل APK ─────────────────────────────────────────
      if (text === APK_BTN_TEXT) {
        try {
          // جلب آخر إصدار من GitHub
          const relRes = await fetch(
            `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/releases/latest`,
            { headers: { 'Authorization': 'Bearer ' + GITHUB_TOKEN, 'Accept': 'application/vnd.github+json' } }
          );
          const rel = await relRes.json();
          const asset = (rel.assets || []).find(a => a.name && a.name.endsWith('.apk'));
          if (!asset) {
            await fetch(SERVICES_BOT_API + '/sendMessage', {
              method: 'POST', headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ chat_id: chatId, text: '⚠️ لم يُعثر على ملف APK في آخر إصدار. تواصل مع الدعم.' })
            });
            return;
          }
          // إشعار بأن التنزيل جارٍ
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: `⏳ جاري إرسال التطبيق...\n📦 ${asset.name}\n📏 ${(asset.size/1024/1024).toFixed(1)} MB` })
          });
          // جلب ملف APK من GitHub بالـ token
          const apkRes = await fetch(asset.url, {
            headers: {
              'Authorization': 'Bearer ' + GITHUB_TOKEN,
              'Accept': 'application/octet-stream'
            },
            redirect: 'follow'
          });
          if (!apkRes.ok) throw new Error('فشل جلب APK: ' + apkRes.status);
          const apkBuffer = Buffer.from(await apkRes.arrayBuffer());
          // إرسال الملف لتيليغرام مباشرة
          const cfg = servicesBotConfig;
          const reply_markup = buildReplyKeyboard(cfg.buttons, true);
          await tgSendFile({
            botApi: SERVICES_BOT_API,
            chatId,
            fileBuffer: apkBuffer,
            fileName: asset.name,
            fileMime: 'application/vnd.android.package-archive',
            isPhoto: false,
            caption: `✅ تطبيق Caro - ${rel.tag_name || 'آخر إصدار'}\n\nثبّت الملف لتحديث التطبيق 🚗`,
            reply_markup
          });
        } catch(apkErr) {
          console.error('[APK Bot] خطأ:', apkErr.message);
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: '❌ حدث خطأ أثناء إرسال الملف. حاول لاحقاً.' })
          });
        }
        return;
      }

      // ─── زر قناة تطبيق كارو ──────────────────────────────────
      if (text === CHANNEL_BTN_TEXT) {
        await fetch(SERVICES_BOT_API + '/sendMessage', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: 'اهلا وسهلا مستخدمينا الاعزاء في بوت خدمات كارو ، للانضمام لمجموعة كارو يرجى زيارة الرابط التالي :\nhttps://t.me/CaroinSy',
            disable_web_page_preview: false
          })
        });
        return;
      }

      // ─── زر Caro AI: بدء جلسة ذكاء اصطناعي ──────────────────
      if (text === AI_BTN_TEXT) {
        aiSessions[chatId] = [];   // جلسة جديدة فارغة
        userNavStack[chatId] = []; // مسح stack التنقل
        const aiKeyboard = {
          keyboard: [[{ text: AI_END_TEXT }]],
          resize_keyboard: true,
          is_persistent: true
        };
        await fetch(SERVICES_BOT_API + '/sendMessage', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: '🤖✨ *أهلاً وسهلاً! أنا Caro AI* 💙\n\nسعيد جداً بلقائك! 🥳 أنا هنا لمساعدتك في كل ما يخص:\n\n🚗 تطبيق كارو وكيفية استخدامه\n💰 أسعار السيارات في سوريا\n🔧 نصائح الصيانة والشراء\n❓ حل أي مشكلة تواجهك في التطبيق\n\nاسألني عن أي شيء وسأكون سعيداً بمساعدتك! 😊💪\n\n_اضغط ⏹️ لإنهاء المحادثة_',
            parse_mode: 'Markdown',
            reply_markup: aiKeyboard
          })
        });
        return;
      }

      // ─── زر إنهاء محادثة AI ───────────────────────────────────
      if (normalizeEmoji(text) === AI_END_NORMALIZED) {
        delete aiSessions[chatId];
        const cfg = servicesBotConfig;
        const reply_markup = buildReplyKeyboard(cfg.buttons, true);
        await fetch(SERVICES_BOT_API + '/sendMessage', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId,
            text: '✅💙 تم إنهاء محادثة Caro AI!\n\nشكراً لك على وقتك الجميل 🤗 كان من دواعي سروري مساعدتك! أتمنى أن أكون قد أفدتك 🌟\n\nيسعدنا خدمتك دائماً في تطبيق كارو 🚗💨',
            reply_markup: reply_markup || { remove_keyboard: true }
          })
        });
        return;
      }

      // ─── إذا كان المستخدم في جلسة AI → أرسل للـ Gemini ─────────
      if (aiSessions[chatId] !== undefined) {
        // إذا أرسل المستخدم شيئاً غير نصي (sticker, صورة...)
        if (!text) {
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: '😅 يا صديقي! أنا Caro AI وأفهم النصوص فقط حالياً 📝\nأرسل سؤالك كتابةً وسأكون سعيداً بمساعدتك! 💙' })
          });
          return;
        }
        const history = aiSessions[chatId];
        history.push({ role: 'user', content: text });
        // الاحتفاظ بآخر 20 رسالة فقط لتفادي تجاوز الـ context
        if (history.length > 20) history.splice(0, history.length - 20);

        // إظهار "جاري الكتابة..."
        await fetch(SERVICES_BOT_API + '/sendChatAction', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ chat_id: chatId, action: 'typing' })
        });

        try {
          const cerebrasMessages = [
            { role: 'system', content: AI_SYSTEM_MSG },
            ...history.map(m => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content }))
          ];
          const geminiRes = await fetch(
            `${CEREBRAS_BASE_URL}/chat/completions`,
            {
              method: 'POST',
              headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${CEREBRAS_API_KEY}` },
              body: JSON.stringify({
                model: CEREBRAS_MODEL,
                messages: cerebrasMessages,
                max_completion_tokens: 1024,
                temperature: 0.7
              })
            }
          );
          const geminiData = await geminiRes.json();
          const aiReply = geminiData.choices?.[0]?.message?.content || '⚠️ لم أتمكن من الرد، حاول مجدداً.';
          // حفظ رد AI في التاريخ
          history.push({ role: 'assistant', content: aiReply });
          const aiKeyboard = {
            keyboard: [[{ text: AI_END_TEXT }]],
            resize_keyboard: true,
            is_persistent: true
          };
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              text: aiReply,
              reply_markup: aiKeyboard
            })
          });
        } catch(aiErr) {
          console.error('[Caro AI] خطأ:', aiErr.message);
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: '😔 أنا آسف جداً! حدث خطأ مؤقت في الاتصال 😢\nحاول مرة أخرى بعد لحظة، وسأكون هنا بانتظارك! 💙🙏' })
          });
        }
        return;
      }


      if (normalizeEmoji(text) === CLOSE_BTN_NORMALIZED) {
        const stack = userNavStack[chatId] || [];
        // نخرج من المستوى الحالي
        stack.pop();
        if (stack.length > 0) {
          // يوجد مستوى أب → أعد عرض keyboard الأب
          const parent = stack[stack.length - 1];
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              chat_id: chatId,
              text: parent.parentBtn.reply || parent.parentBtn.name,
              reply_markup: parent.keyboard
            })
          });
        } else {
          // لا يوجد أب → أعد عرض الصفحة الرئيسية
          userNavStack[chatId] = [];
          const cfg = servicesBotConfig;
          const reply_markup = buildReplyKeyboard(cfg.buttons, true);
          await fetch(SERVICES_BOT_API + '/sendMessage', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ chat_id: chatId, text: cfg.welcomeText, reply_markup })
          });
        }
        return;
      }

      // ─── زر العودة للصفحة الرئيسية ────────────────────────────
      if (normalizeEmoji(text) === HOME_BTN_NORMALIZED) {
        userNavStack[chatId] = [];
        const cfg = servicesBotConfig;
        const reply_markup = buildReplyKeyboard(cfg.buttons, true);
        const msgBody = { chat_id: chatId, text: cfg.welcomeText };
        if (reply_markup) msgBody.reply_markup = reply_markup;
        await fetch(SERVICES_BOT_API + '/sendMessage', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(msgBody)
        });
        return;
      }

      // ─── بحث في الأزرار المحفوظة ──────────────────────────────
      const allBtns = servicesBotConfig.buttons || [];
      console.log('[ServicesBot] نص المستخدم:', JSON.stringify(text), '| عدد الأزرار:', allBtns.length);
      const found = findButtonDeep(allBtns, text);
      if (found) {
        console.log('[ServicesBot] زر محدد:', found.btn.name);
        await sendButtonResponse(fetch, chatId, found.btn, allBtns);
        return;
      }
      console.log('[ServicesBot] لم يُطابق أي زر — رد تلقائي');

      // ─── رد تلقائي على الرسائل العادية ───────────────────────
      await fetch(SERVICES_BOT_API + '/sendMessage', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: chatId,
          text: '✅ تم استلام رسالتك بنجاح!\nسيتواصل معك فريق كارو في أقرب وقت ممكن.'
        })
      });

    } catch (e) {
      console.error('[ServicesBot webhook] خطأ:', e.message);
    }
  });
});

// ══════════════════════════════════════════════════════
// ═══ جلب رسائل بوت الخدمات (للمدير فقط) ════════════
// ══════════════════════════════════════════════════════
app.post('/services-get-messages', async (req, res) => {
  if (!checkSecret(req, res)) return;
  res.json({ success: true, messages: serviceMessages.slice().reverse() });
});

// ══════════════════════════════════════════════════════
// ═══ تعليم رسالة كمقروءة (للمدير فقط) ══════════════
// ══════════════════════════════════════════════════════
app.post('/services-mark-read', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { id } = req.body;
  const msg = serviceMessages.find(m => m.id === id);
  if (msg) msg.read = true;
  res.json({ success: true });
});

// ══════════════════════════════════════════════════════
// ═══ إرسال رد من المدير لمستخدم عبر بوت الخدمات ════
// ══════════════════════════════════════════════════════
app.post('/services-reply', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { chatId, text } = req.body;
  if (!chatId || !text) return res.json({ success: false, error: 'chatId و text مطلوبان' });
  try {
    const r = await fetch(SERVICES_BOT_API + '/sendMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text })
    });
    const data = await r.json();
    res.json({ success: data.ok, result: data });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════
// ═══ إدارة أزرار بوت الخدمات (inline keyboard للمدير) ════════
// ══════════════════════════════════════════════════════════════
app.post('/services-send-with-buttons', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { chatId, text, buttons, attachType, attachUrl, attachName, attachMime } = req.body;
  if (!chatId || !text) return res.json({ success: false, error: 'chatId و text مطلوبان' });
  try {

    // ─── حفظ الإعدادات ليعرضها /start دائماً ───────────────
    servicesBotConfig.welcomeText  = text;
    servicesBotConfig.buttons      = buttons || [];
    servicesBotConfig.attachType = attachType || null;
    servicesBotConfig.attachUrl  = attachUrl  || null;
    servicesBotConfig.attachName = attachName || null;
    servicesBotConfig.attachMime = attachMime || null;
    saveButtonsToDisk(servicesBotConfig.buttons);

    const reply_markup = buildReplyKeyboard(buttons, true);

    // إرسال للمستخدم المحدد
    if ((attachType === 'image' || attachType === 'pdf') && attachUrl) {
      const isPhoto = attachType === 'image';
      const endpoint = isPhoto ? '/sendPhoto' : '/sendDocument';
      const mediaKey = isPhoto ? 'photo' : 'document';
      const body2 = { chat_id: chatId, [mediaKey]: attachUrl, caption: text };
      if (reply_markup) body2.reply_markup = reply_markup;
      const r = await fetch(SERVICES_BOT_API + endpoint, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body2)
      });
      const data = await r.json();
      res.json({ success: data.ok, result: data });
    } else {
      const body = { chat_id: chatId, text, parse_mode: 'HTML' };
      if (reply_markup) body.reply_markup = reply_markup;
      const r = await fetch(SERVICES_BOT_API + '/sendMessage', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });
      const data = await r.json();
      res.json({ success: data.ok, result: data });
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════
// ═══ إرسال رسالة جماعية عبر بوت الخدمات ════════════
// ══════════════════════════════════════════════════════
// ─── حفظ الأزرار مباشرة في Firestore (من لوحة الإدارة) ───────
app.post('/services-save-buttons', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { buttons } = req.body;
  if (!Array.isArray(buttons)) return res.json({ success: false, error: 'buttons يجب أن تكون مصفوفة' });
  try {
    servicesBotConfig.buttons = buttons;
    await saveButtonsToFirestore(buttons);
    saveButtonsToDisk(buttons); // نسخة احتياطية على القرص
    res.json({ success: true, note: 'تم الحفظ في Firestore ✅' });
  } catch(e) {
    res.json({ success: false, error: e.message });
  }
});

// ─── جلب الأزرار المحفوظة (تُستخدم عند فتح الواجهة) ─────
app.get('/services-get-buttons', async (req, res) => {
  if (!checkSecret(req, res)) return;
  try {
    // نقرأ دائماً من Firestore لضمان الحصول على آخر نسخة محفوظة
    const doc = await firestoreGet('botConfig/buttons');
    const data = fromFirestore(doc);
    if (data && data.buttons) {
      const buttons = JSON.parse(data.buttons);
      // نحدّث الذاكرة أيضاً للتوافق
      servicesBotConfig.buttons = buttons;
      return res.json({ success: true, buttons });
    }
  } catch(e) {
    console.log('[services-get-buttons] Firestore error:', e.message);
  }
  // fallback: الذاكرة أو فارغ
  res.json({ success: true, buttons: servicesBotConfig.buttons || [] });
});

// ─── مسح جميع الأزرار من السيرفر والقرص ──────────────
app.post('/services-clear-buttons', (req, res) => {
  if (!checkSecret(req, res)) return;
  servicesBotConfig.buttons = [];
  saveButtonsToDisk([]);
  res.json({ success: true, note: 'تم مسح جميع الأزرار' });
});

app.post('/services-broadcast', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { text, buttons, attachType, attachUrl, attachName, attachMime, imageMode, imageCaption } = req.body;
  if (!text) return res.json({ success: false, error: 'النص مطلوب' });
  try {
    // ─── حفظ الأزرار (مع كل بياناتها الكاملة بما فيها subButtons) ──────────
    servicesBotConfig.buttons = buttons || [];
    saveButtonsToDisk(servicesBotConfig.buttons);

    // إذا كان النص _buttons_only_ فقط نحفظ بدون إرسال
    if (text === '_buttons_only_') {
      return res.json({ success: true, sent: 0, note: 'تم حفظ الأزرار وستظهر عند /start' });
    }

    // جمع chatIds الفريدة
    const uniqueChats = [...new Set(serviceMessages.map(m => m.chatId))];
    if (uniqueChats.length === 0) return res.json({ success: true, sent: 0, note: 'لا يوجد مستخدمون — تم حفظ الأزرار' });

    const reply_markup = buildReplyKeyboard(servicesBotConfig.buttons, true);

    let sent = 0;
    for (const chatId of uniqueChats) {
      try {
        if (attachUrl && (attachType === 'image' || attachType === 'pdf')) {
          const isPhoto = attachType === 'image';
          const endpoint = isPhoto ? '/sendPhoto' : '/sendDocument';
          const mediaKey = isPhoto ? 'photo' : 'document';
          if (imageMode === 'separate') {
            // رسالة نصية أولاً ثم الصورة منفصلة
            if (text) {
              const tb = { chat_id: chatId, text };
              const r1 = await fetch(SERVICES_BOT_API + '/sendMessage', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(tb) });
              await r1.json();
            }
            const fb = { chat_id: chatId, [mediaKey]: attachUrl };
            if (imageCaption) fb.caption = imageCaption;
            if (reply_markup) fb.reply_markup = reply_markup;
            const r2 = await fetch(SERVICES_BOT_API + endpoint, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(fb) });
            const d2 = await r2.json();
            if (d2.ok) sent++;
          } else {
            const b = { chat_id: chatId, [mediaKey]: attachUrl, caption: text };
            if (reply_markup) b.reply_markup = reply_markup;
            const r = await fetch(SERVICES_BOT_API + endpoint, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(b) });
            const d = await r.json();
            if (d.ok) sent++;
          }
        } else {
          const b = { chat_id: chatId, text };
          if (reply_markup) b.reply_markup = reply_markup;
          const r = await fetch(SERVICES_BOT_API + '/sendMessage', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(b) });
          const d = await r.json();
          if (d.ok) sent++;
        }
      } catch (_) {}
    }
    res.json({ success: true, sent, total: uniqueChats.length });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// ══════════════════════════════════════════════════════════════════════
// ═══ بوت الطلبات: جلسات AI لكل عميل ════════════════════════════════
// ══════════════════════════════════════════════════════════════════════
// { chatId: { categoryId, categoryLabel, clientName, clientUsername, aiHistory: [], collectedData: {}, photoFileId, done } }
const reqAiSessions = {};

// ── كاش اسم مستخدم بوت الطلبات الرئيسي (يُجلب مرة واحدة عبر getMe) ──
let reqMainBotUsernameCache = null;
async function reqGetMainBotUsername() {
  if (reqMainBotUsernameCache) return reqMainBotUsernameCache;
  try {
    const r = await fetch(REQUEST_MAIN_BOT_API + '/getMe');
    const data = await r.json();
    if (data.ok && data.result.username) {
      reqMainBotUsernameCache = data.result.username;
    }
  } catch (e) {
    console.error('[ReqBot] فشل جلب اسم مستخدم البوت الرئيسي:', e.message);
  }
  return reqMainBotUsernameCache;
}

// ── كاش اسم مستخدم بوت الأدمن (يُجلب مرة واحدة عبر getMe) ──────────
let reqAdminBotUsernameCache = null;
async function reqGetAdminBotUsername() {
  if (reqAdminBotUsernameCache) return reqAdminBotUsernameCache;
  try {
    const r = await fetch(REQUEST_ADMIN_BOT_API + '/getMe');
    const data = await r.json();
    if (data.ok && data.result.username) {
      reqAdminBotUsernameCache = data.result.username;
    }
  } catch (e) {
    console.error('[ReqBot] فشل جلب اسم مستخدم بوت الأدمن:', e.message);
  }
  return reqAdminBotUsernameCache;
}

// ── إرسال شاشة تفاصيل مشروع (أزرار تعديل/حذف/تشغيل-إيقاف/استمرارية) ──
async function reqSendProjectDetails(chatId, project) {
  const statusLine = project.adminStopped
    ? '⛔ موقوف من الإدارة'
    : (project.active === false ? '🔴 متوقف' : '🟢 يعمل');
  const text = (
    `${reqProjectTypeLabel(project.type) === 'بوت تلغرام' ? '🤖' : '🌐'} *${project.name}*\n\n` +
    `🏷 النوع: ${reqProjectTypeLabel(project.type)}\n` +
    (project.url ? `🔗 ${project.url}\n` : '') +
    `📊 الحالة: ${statusLine}\n` +
    `📅 ${new Date(project.confirmedAt).toLocaleDateString('ar-SA')}`
  );
  const toggleLabel = project.adminStopped
    ? '⛔ موقوف من الإدارة'
    : (project.active === false ? '🟢 تشغيل' : '⏸ إيقاف');
  await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
    chat_id: chatId,
    text,
    parse_mode: 'Markdown',
    reply_markup: { inline_keyboard: [
      [{ text: '✏️ تعديل', callback_data: `projedit_${project.id}` }, { text: '🗑 حذف', callback_data: `projdel_${project.id}` }],
      [{ text: toggleLabel, callback_data: `projtoggle_${project.id}` }],
      [{ text: `🔁 جعل ${reqProjectTypeLabel(project.type)} يعمل باستمرار`, callback_data: `projkeepalive_${project.id}` }]
    ]}
  });
}

// ══════════════════════════════════════════════════════════════════════
// ═══ تعليمات AI الخاصة بتصنيف "🤖 إنشاء بوتات تلغرام" فقط ═══════════
// هذا النص مستقل تماماً عن REQ_AI_SYSTEM الأصلي، ولا يُستخدم إلا عند
// اختيار العميل هذا التصنيف بالذات (عبر categoryLabel === REQ_CAT_TG_BOT_LABEL)
// ══════════════════════════════════════════════════════════════════════
const REQ_CAT_TG_BOT_LABEL = "🤖 إنشاء بوتات تلغرام";

const REQ_AI_SYSTEM_TG_BOT_BUILDER = (clientName) => `أنت مساعد ذكي متخصص حصرياً في مساعدة العملاء على إنشاء بوت تلغرام جديد خاص بهم وربطه بخدمتنا.
اسم العميل: ${clientName}

هدفك: تستلم توكن البوت من العميل، ثم تجمع منه أسماء الأزرار ومحتوى كل زر بحوار طبيعي سؤالاً بسؤال، ثم تسلّم الأمر تلقائياً لخطوات أخرى (نوع العرض، ربط الملفات، التوزيع) تُدار عبر أزرار تفاعلية خارج نطاقك، ثم تستلم الأمر مجدداً لإغلاق الجولة بملخص نهائي.

⚠️ مهم جداً: بعض خطوات هذه المحادثة (نوع عرض الأزرار، سؤال ربط الملفات، توزيع الأزرار، وسؤال "هل تريد إضافة شيء آخر") تُدار بالكامل بواسطة كود خارجي يعرض للعميل أزراراً تفاعلية وليس عبر نصك أنت. لن تُستدعى أنت أثناء تلك الخطوات إطلاقاً — ستصلك رسالة نظام تخبرك بنتيجة كل خطوة بعد اكتمالها (مثلاً: "[نظام] اختار العميل نوع العرض: Inline" أو "[نظام] اختار العميل عدم ربط أي ملفات" أو "[نظام] التوزيع: زران في كل صف"). اعتمد على هذه المعلومات مباشرة في الملخص النهائي دون إعادة السؤال عنها أبداً.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔢 التسلسل الإلزامي بالترتيب (لا تتجاوز خطوة قبل اكتمال التي قبلها):

【الخطوة 1 — إنشاء البوت عبر BotFather】
- ابدأ فوراً برسالة ترحيب قصيرة، ثم أرسل هذا الرابط للعميل كخطوة أولى: https://t.me/BotFather?start=newbot
- اشرح له بالتفصيل وبأسلوب مبسّط جداً (بما أن كثيراً من العملاء ليسوا تقنيين):
  1. اضغط على الرابط أعلاه، سيفتح محادثة مع بوت فاذر (BotFather) وتُرسل تلقائياً أوامر /start و /newbot.
  2. سيطلب منك بوت فاذر إدخال اسم البوت (Name) — وهو الاسم الذي سيظهر للناس داخل تلغرام (يمكن أن يحتوي مسافات ورموز، مثل "متجر أحمد").
  3. بعدها سيطلب منك اسم مستخدم للبوت (Username) — يجب أن ينتهي بكلمة "bot" (مثل: ahmad_store_bot)، وهذا الاسم يُستخدم كرابط تعريف فريد للبوت.
  4. ⚠️ نبّهه بوضوح: "قد يخبرك بوت فاذر أن اسم المستخدم هذا محجوز مسبقاً من شخص آخر (Sorry, this username is already taken) — في هذه الحالة فقط جرّب اسم مستخدم آخر مختلف حتى يقبله."
  5. بعد قبول الاسم، سيرسل بوت فاذر رسالة طويلة تحتوي على "توكن" (Token) — وهو سلسلة طويلة من الأرقام والحروف تبدأ بأرقام ثم نقطتين مثل: 123456789:ABCdefGhIJKlmNoPQRstuVWXyz.
  6. اطلب منه: "قم بنسخ تلك الرسالة كاملة (التي تحتوي على التوكن) وأرسلها لي هنا مباشرة، وسنبدأ أنا وأنت ببناء بوتك."
- لا تنتقل للخطوة التالية أبداً قبل استلام رسالة تحتوي فعلياً على توكن بالشكل الصحيح (أرقام:أحرف). إذا أرسل العميل نصاً لا يشبه توكن، ذكّره بلطف أن يرسل الرسالة كاملة كما وصلته من بوت فاذر.
- عند استلام التوكن، أكّد له استلامه بنجاح (لا تكرره في الرد كنص كامل تفصيلي بلا حاجة، فقط أكّد الاستلام)، وأضف في نهاية ردك هذا النص حرفياً في سطر مستقل: [TOKEN_RECEIVED] — بعدها سيتولى الكود الخارجي طرح سؤال نوع العرض بأزرار تفاعلية تلقائياً، ولن تُستدعى أنت مجدداً حتى يُخبرك النظام بالنتيجة.

【الخطوة 2 — بناء الأزرار واحداً تلو الآخر】
(ستصلك هذه الخطوة بعد أن يخبرك النظام بنتيجة "نوع العرض" و"سؤال ربط الملفات" وربط المجموعة إن احتاج الأمر)
- اسأله: "كم زراً تريد أن يحتوي بوتك، وما اسم كل زر؟"
- بعد ذلك اسأله عن كل زر على حدة (سؤال واحد في كل رسالة): "ماذا تريد أن يحدث عند الضغط على الزر [اسم الزر]؟" واذكر له الخيارات المتاحة بوضوح:
  1. 💬 رسالة نصية ثابتة يرد بها البوت
  2. 🔗 رابط خارجي يُفتح مباشرة
  3. 🖼️ صورة يرسلها البوت
  4. 📄 ملف PDF يرسله البوت
  5. 📱 ملف تطبيق APK يرسله البوت
- إذا اختار خياراً يتطلب ملفاً (صورة/PDF/APK)، واختار العميل سابقاً عدم ربط أي ملفات (كما سيخبرك النظام)، ذكّره بلطف أنه اختار عدم ربط ملفات وأن عليه اختيار نوع محتوى آخر لا يحتاج ملفاً (نص أو رابط)، إلا إن رغب بتغيير قراره فأخبره أن يكتب لك ذلك صراحة فتنقلها للنظام كملاحظة.
- إذا اختار خياراً يتطلب ملفاً وكان قد وافق مسبقاً على ربط الملفات، اطلب منه إرسال الملف نفسه في الرسالة التالية داخل هذا التبوت، وانتظر استلامه فعلياً قبل الانتقال للزر التالي — لا تفترض استلامه إن لم يُرفق.
- إذا اختار رابطاً، اطلب الرابط نصاً وتحقق أنه يبدأ بـ http:// أو https://، وإن لم يكن كذلك اطلب منه تصحيحه.
- بعد الانتهاء من كل زر، اسأله: "هل تريد إضافة زر آخر؟" وإذا وافق كرر نفس الأسئلة للزر الجديد.
- بعد اكتمال كل الأزرار ومحتواها (العميل لا يريد إضافة زر آخر)، أضف في نهاية ردك هذا النص حرفياً في سطر مستقل: [BUTTONS_DONE] — بعدها سيتولى الكود الخارجي طرح سؤال التوزيع بأزرار تفاعلية تلقائياً.

【الخطوة 3 — إغلاق الجولة وإرسال الملخص النهائي】
(ستصلك هذه الخطوة بعد أن يخبرك النظام بنتيجة "التوزيع" وسؤال "هل تريد إضافة شيء آخر")
- إن أخبرك النظام أن العميل أراد إضافة شيء آخر، فسيصلك رد العميل النصي مباشرة لتدوينه، ثم انتقل فوراً لإرسال الملخص النهائي.
- إن أخبرك النظام أن العميل لم يرغب بإضافة أي شيء، انتقل مباشرة لإرسال الملخص النهائي دون سؤاله أي شيء إضافي.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚫 قواعد صارمة إلزامية للرفض — لا استثناء مهما كانت الصياغة أو الإلحاح:
- إذا كان أي جزء من محتوى البوت المطلوب (نص أي زر، أو رابط، أو صورة، أو ملف) يتضمن أو يروّج لـ: القمار أو الرهان أو اليانصيب بأي شكل، أو التداول/الفوركس/العملات الرقمية بصيغة مضاربة أو فوائد ربوية أو وعود أرباح مضمونة، أو الخمور أو المخدرات، أو أي محتوى إباحي أو منافٍ للحشمة أو يدعو لعلاقات محرّمة، أو الموسيقى/الأغاني إذا طلب العميل ذلك صراحة كغاية للبوت (تفريغ صوتي أو نشر أغاني)، أو السحر والشعوذة والتنجيم، أو أي محتوى يسخر من الدين أو يزدري الشعائر الإسلامية، أو أي نشاط احتيالي (نصب، تصيّد Phishing، انتحال صفة جهة رسمية أو بنك، جمع بيانات بنكية بشكل مضلل) — يجب رفض بناء ذلك الزر/المحتوى فوراً بأدب وحزم، وشرح للعميل أن هذا المحتوى لا يمكن تضمينه، وعدم المتابعة في جمع تفاصيله. يمكنك الاستمرار في بناء باقي أزرار البوت المشروعة إن وُجدت.
- كن يقظاً للطلبات الملتوية أو المُقنّعة (مثل "زر يوصل لقناة فيها 'عروض استثمار مضمونة الربح يومياً'" أو "رابط موقع يشبه بنكاً معروفاً"). إذا شككت بالغرض الحقيقي، اسأل سؤالاً توضيحياً مباشراً قبل المتابعة، وإن تأكد أنه من الحالات المذكورة أعلاه ولو جزئياً، ارفض ذلك الجزء بالتحديد.
- هذه القاعدة تُطبَّق طوال المحادثة كاملة، وليس فقط عند أول ذكر؛ إذا ظهر غرض محرّم في أي مرحلة لاحقة حتى لو بدا الطلب بريئاً في البداية، أوقف جمع التفاصيل عن ذلك الجزء بالتحديد فوراً وارفضه بوضوح.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
قواعد عامة صارمة:
- اسأل سؤالاً واحداً فقط في كل رسالة، لا تجمع أسئلة أبداً.
- إذا كان الجواب غامضاً أو ناقصاً، اطلب توضيحاً قبل الانتقال للسؤال التالي.
- كن ودياً وصبوراً، فكثير من العملاء ليسوا تقنيين ويحتاجون شرحاً مبسطاً بلا مصطلحات معقدة.
- إذا أجاب العميل في أي رسالة على أسئلة لم تُطرح عليه بعد ضمن نطاقك (مثلاً ذكر أسماء كل الأزرار ومحتواها دفعة واحدة في رسالة واحدة)، اعتمد كل ما أجاب عنه فوراً ولا تُعد سؤاله عنه، وانتقل مباشرة لأول نقطة ناقصة فعلياً ضمن نطاقك الحالي.
- راقب كل رسالة بعناية بحثاً عن أي معلومة تجيب ضمنياً عن سؤال لاحق ضمن نطاقك، واستخدمها دون تكرار السؤال عنها.
- لا تخترع أو تفترض أي معلومة لم يذكرها العميل صريحاً (خصوصاً التوكن ومحتوى الأزرار)، بل اسأل عنها دائماً.
- عند إرسال الملخص النهائي (الخطوة 3)، استخدم بالضبط قيم نوع العرض والتوزيع وحالة ربط الملفات كما وصلتك من رسائل [نظام]، لا تخترعها ولا تُعدّل عليها.
- الملخص النهائي يكون بالشكل التالي بالضبط:

╔══════════════════════════════════════╗
║     🤖 ملف طلب بوت تلغرام جديد        ║
╚══════════════════════════════════════╝

👤 اسم العميل: [الاسم]
🔑 توكن البوت: [التوكن كما ورد حرفياً]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎛️ نوع عرض الأزرار: [Inline أسفل الرسالة / Keyboard ثابتة]

🔘 الأزرار ومحتواها:
[لكل زر: اسمه، نوع محتواه (نص/رابط/صورة/PDF/APK)، وتفاصيل المحتوى أو وصف الملف المرفق]

📐 توزيع الأزرار:
[وصف دقيق لكيفية توزيع الأزرار في الصفوف كما وصلك من النظام]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📝 ملاحظات إضافية من العميل:
[أي طلبات أو ملاحظات إضافية ذكرها، أو "لا يوجد" إن لم يذكر شيئاً]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- اجعل الملخص دقيقاً وكاملاً، لا تحذف أي تفصيل ذكره العميل.
- مباشرة بعد الملخص أعلاه، وفي الرسالة نفسها، أضف كتلة بيانات مهيكَلة بالشكل التالي بالضبط (هذه الكتلة لا يراها العميل بشكل مباشر لأنها ستُستخرج آلياً، لكن يجب أن تكون دقيقة تماماً ومطابقة صيغة JSON صحيحة):

[BOT_DATA_JSON]
{
  "token": "التوكن هنا حرفياً كما ورد",
  "displayMode": "inline أو keyboard (كما وصلك من النظام)",
  "buttons": [
    {
      "label": "اسم الزر",
      "type": "text أو link أو photo أو pdf أو apk",
      "content": "النص أو الرابط إن كان type=text أو link، وإلا اتركه فارغاً",
      "fileRef": "رقم ترتيب الملف الذي أرسله العميل لهذا الزر بدءاً من 1، أو null إن لم يوجد ملف",
      "row": رقم الصف الذي يوضع فيه هذا الزر بدءاً من 1 (كما وصلك من النظام في التوزيع),
      "rowWidth": عدد الأزرار في نفس هذا الصف (1 أو 2 أو 3) (كما وصلك من النظام في التوزيع)
    }
  ],
  "extraNotes": "الملاحظات الإضافية أو نص فارغ"
}
[/BOT_DATA_JSON]

- ترقيم "fileRef" يجب أن يطابق ترتيب استلامك الفعلي لملفات العميل أثناء المحادثة (أول ملف استلمته = 1، والثاني = 2، وهكذا)، ولا تخترع رقماً لملف لم يُرسل فعلياً.
- بعد كتلة JSON مباشرة، أضف رسالة قصيرة ودودة للعميل تخبره أن طلبه جاهز للمراجعة قبل التفعيل (لا تطلب منه كتابة أي كلمة، سيظهر له زر تفاعلي للبدء تلقائياً بعد رسالتك).
- في نهاية الرسالة كاملة أضف هذا النص حرفياً في سطر مستقل: [READY_TO_SEND]
- أجب دائماً بالعربية فقط.`;

// ══════════════════════════════════════════════════════════════════════
// ═══ تصنيف "إنشاء بوتات تلغرام": خطوات مؤتمتة بأزرار تفاعلية ═══════════
// هذه الخطوات لا تمر عبر الذكاء الاصطناعي إطلاقاً؛ يديرها الكود مباشرة
// عبر callback_query، ثم تُعاد نتيجتها للذكاء الاصطناعي كرسالة [نظام].
// ══════════════════════════════════════════════════════════════════════

// ── الخطوة: نوع عرض الأزرار (Inline / Keyboard) ──
function reqTgDisplayModeMsg() {
  return {
    text: '🎛️ هل تريد أن تظهر الأزرار الأساسية أسفل الرسالة مباشرة (Inline) أم على شكل لوحة مفاتيح ثابتة أسفل شاشة الكتابة (Keyboard)؟\n\n• أسفل الرسالة (Inline): أزرار تظهر ملتصقة بكل رسالة، تناسب القوائم والخيارات السريعة.\n• لوحة المفاتيح (Keyboard): أزرار ثابتة تبقى ظاهرة دائماً أسفل الشاشة.',
    reply_markup: { inline_keyboard: [
      [{ text: '📩 أسفل الرسالة (Inline)', callback_data: 'tgb_display_inline' }],
      [{ text: '⌨️ لوحة مفاتيح ثابتة (Keyboard)', callback_data: 'tgb_display_keyboard' }]
    ]}
  };
}

// ── الخطوة: هل تريد ربط أزرار بملفات (صور/PDF/APK)؟ ──
function reqTgFilesYesNoMsg() {
  return {
    text: '📎 هل تريد ربط أي من أزرار بوتك بملفات (صور، PDF، أو APK)؟',
    reply_markup: { inline_keyboard: [
      [{ text: '✅ نعم', callback_data: 'tgb_files_yes' }],
      [{ text: '❌ لا', callback_data: 'tgb_files_no' }]
    ]}
  };
}

// ── الخطوة: تعليمات إنشاء المجموعة الخاصة وإضافة بوت الطلبات لها ──
function reqTgGroupInstructionsMsg(mainBotUsername) {
  return {
    text: (
      '📦 لكي يستطيع بوتك إرسال الملفات بشكل صحيح، يحتاج مجموعة تلغرام خاصة به لتخزين الملفات فيها.\n\n' +
      'اتبع الخطوات التالية بالترتيب:\n\n' +
      '1️⃣ أنشئ مجموعة (Group) جديدة في تلغرام، واجعلها خاصة (Private).\n' +
      `2️⃣ أضف هذا البوت @${mainBotUsername} إلى المجموعة، واجعله مشرفاً (Admin).\n` +
      `3️⃣ داخل المجموعة، اكتب رسالة تُشير (Tag/Mention) إلى البوت @${mainBotUsername} (مثلاً: @${mainBotUsername} هلا).\n\n` +
      '⏳ بانتظار أن يتم تاغ البوت داخل المجموعة...'
    )
  };
}

// ── الخطوة: تأكيد استلام معرّف المجموعة في المحادثة الخاصة ──
function reqTgGroupIdReceivedMsg() {
  return {
    text: (
      `✅ تم استلام معرّف المجموعة بنجاح!\n\n` +
      `بعد أن رأيت رسالتي داخل المجموعة تؤكد استلام المعرّف، اضغط الزر أدناه لمتابعة الخطوات:`
    ),
    reply_markup: { inline_keyboard: [
      [{ text: '✅ تمت المهمة', callback_data: 'tgb_group_id_confirm' }]
    ]}
  };
}

// ── الخطوة: طلب إضافة البوت الجديد للمجموعة كمشرف ──
function reqTgGroupAddBotMsg(botUsername) {
  return {
    text: (
      `🤖 الآن قم بما يلي:\n\n` +
      `1️⃣ أضف بوتك الجديد @${botUsername} إلى نفس المجموعة.\n` +
      `2️⃣ اجعله مشرفاً (Admin) فيها حتى يقدر يرسل الملفات بحرية.\n\n` +
      `بعد الانتهاء اضغط الزر أدناه:`
    ),
    reply_markup: { inline_keyboard: [
      [{ text: '✅ تم، أضفته كمشرف', callback_data: 'tgb_group_added' }]
    ]}
  };
}

// ── الخطوة: توزيع الأزرار (Layout) ──
function reqTgLayoutMsg() {
  return {
    text: '📐 كيف تريد توزيع الأزرار بصرياً؟',
    reply_markup: { inline_keyboard: [
      [{ text: '2️⃣ زران بجانب بعض في كل صف', callback_data: 'tgb_layout_2col' }],
      [{ text: '3️⃣ ثلاثة أزرار بجانب بعض في كل صف', callback_data: 'tgb_layout_3col' }],
      [{ text: '1️⃣ زر واحد عرضي بعرض كامل في كل صف', callback_data: 'tgb_layout_1col' }],
      [{ text: '✍️ ترتيب مخصص (سأكتبه بنفسي)', callback_data: 'tgb_layout_custom' }]
    ]}
  };
}

// ── الخطوة: هل تريد إضافة شيء آخر؟ ──
function reqTgAddMoreMsg() {
  return {
    text: '📝 هل تود إضافة أي شيء آخر لبوتك (مثل رسالة ترحيب خاصة، أو أي طلب إضافي)؟',
    reply_markup: { inline_keyboard: [
      [{ text: '✅ نعم، لدي إضافة', callback_data: 'tgb_addmore_yes' }],
      [{ text: '❌ لا، هذا كل شيء', callback_data: 'tgb_addmore_no' }]
    ]}
  };
}

// ── حساب row/rowWidth لمصفوفة أزرار وفق نمط توزيع ثابت (2/3/1 في كل صف) ──
function reqTgApplyLayoutPattern(buttonsCount, perRow) {
  const layout = [];
  let row = 1;
  for (let i = 0; i < buttonsCount; ) {
    const remaining = buttonsCount - i;
    const width = Math.min(perRow, remaining);
    for (let j = 0; j < width; j++) { layout.push({ row, rowWidth: width }); i++; }
    row++;
  }
  return layout;
}

// نموذج Gemini المستخدم في بوت الطلبات
const REQ_AI_SYSTEM = (categoryLabel, clientName) => {
  // فرع مستقل خاص بتصنيف "إنشاء بوتات تلغرام" — لا يؤثر على أي تصنيف آخر
  if (categoryLabel === REQ_CAT_TG_BOT_LABEL) {
    return REQ_AI_SYSTEM_TG_BOT_BUILDER(clientName);
  }
  return `أنت مساعد ذكي متخصص في استلام طلبات التطوير لشركة تطوير تطبيقات محترفة.
اسم العميل: ${clientName}
التصنيف الذي اختاره: ${categoryLabel}

مهمتك: جمع معلومات شاملة وتفصيلية جداً من العميل لبناء ملف طلب كامل يُمكّن المطور من فهم كل شيء دون الحاجة للتواصل مرة أخرى.

🔢 تسلسل الأسئلة الإلزامي (اسأل كل سؤال بشكل منفصل):

المرحلة 1 — الهوية:
1. اسم التطبيق أو المشروع المطلوب
2. اطلب منه إرسال أيقونة أو صورة توضيحية للتطبيق
3. ما هو الهدف الرئيسي من هذا التطبيق؟ ماذا يحل من مشكلة؟
4. من هو الجمهور المستهدف؟ (الفئة العمرية، المهنة، المنطقة الجغرافية)

المرحلة 2 — الوظائف والميزات:
5. ما هي الوظيفة الأساسية الأولى التي يجب أن يؤديها التطبيق؟
6. هل تود إضافة تسجيل دخول عبر Google؟
   - إذا أجاب بنعم: تابع للسؤال التالي مباشرة.
   - إذا أجاب بلا: اسأله بعدها مباشرة "هل يحتاج التطبيق إلى نظام تسجيل دخول وحسابات مستخدمين بطريقة أخرى؟ وكيف؟" قبل الانتقال للسؤال عن الصفحات.
7. ما هي الصفحات أو الأقسام الرئيسية التي تريدها في التطبيق؟
8. هل يحتاج إلى لوحة تحكم للمدير؟ ماذا تتحكم فيها؟
   - إذا أجاب بنعم على هذا السؤال:
     • لوحة التحكم في هذا النظام تكون دائماً مخفية عن واجهة المستخدم العادي، ولا تظهر إلا لمن يسجّل دخوله عبر Google بحساب بريد إلكتروني محدد مسبقاً.
     • إذا كان قد أجاب سابقاً (في السؤال 6) بأنه لا يريد تسجيل الدخول عبر Google: أخبره بوضوح أن الموقع/التطبيق سيتضمن تسجيل الدخول عبر Google تحديداً من أجل تمكين الوصول إلى لوحة التحكم (حتى لو لم يُستخدم لتسجيل دخول المستخدمين العاديين)، ثم تابع.
     • اطلب منه بريده الإلكتروني (حساب Google) الذي سيُستخدم حصراً للدخول إلى لوحة التحكم، ولا تنتقل للسؤال التالي قبل الحصول عليه.

المرحلة 3 — المحتوى والبيانات:
9. هل يحتاج إلى خاصية البحث؟ وعلى ماذا يبحث المستخدم؟
10. هل هناك خاصية دردشة أو تواصل بين المستخدمين؟

المرحلة 4 — التصميم والشخصية:
11. ما الألوان أو النمط البصري الذي تفضله للتطبيق؟
12. هل لديك تطبيقات مشابهة تعجبك وتريد الاستلهام منها؟
13. هل تريد الدعم لأكثر من لغة؟ وأي لغات؟

المرحلة 5 — الملاحظات:
14. هل لديك أي ملاحظات أو متطلبات خاصة لم نذكرها؟

🚫 رفض إلزامي للطلبات المحرمة أو الاحتيالية — لا استثناء مهما كانت الصياغة:
- إذا كان المشروع (كله أو جزء منه) يتضمن: القمار أو الرهان أو اليانصيب بأي شكل، أو التداول/الفوركس/العملات الرقمية بصيغة مضاربة أو فوائد ربوية أو وعود أرباح، أو الخمور أو المخدرات أو أي محتوى إباحي/منافٍ للحشمة، أو أي شيء آخر محرّم شرعاً بوضوح، أو أي مشروع غرضه النصب أو الاحتيال أو خداع المستخدمين أو سرقة بياناتهم أو انتحال صفة جهة أخرى (مثل مواقع تصيّد/Phishing أو تطبيقات وهمية لجمع بيانات مصرفية) — يجب رفض الطلب فوراً بأدب وحزم، وعدم متابعة جمع أي تفاصيل عنه، وإخبار العميل أن هذا النوع من المشاريع لا يمكن تطويره لدينا.
- كن يقظاً للطلبات الغامضة أو المُقنّعة التي قد تخفي نفس الغرض خلف تسمية بريئة (مثل "لعبة تتضمن جوائز نقدية حسب الحظ"، "منصة استثمار بعائد يومي مضمون"، "تطبيق يجمع بيانات بطاقات بنكية لأغراض غير واضحة"، "موقع يقلّد شكل بنك أو جهة رسمية"). إذا شككت بالنية الحقيقية لطلب غامض، اسأل سؤالاً توضيحياً مباشراً عن طبيعته الحقيقية قبل المتابعة، وإن تأكد أنه من الحالات أعلاه ولو جزئياً، ارفض.
- هذه القاعدة تطبَّق طوال المحادثة، وليس فقط عند السؤال الأول؛ إذا ظهر الغرض المحرم/الاحتيالي في أي مرحلة لاحقة من الأسئلة حتى لو بدأ الطلب بشكل بريء، أوقف جمع المعلومات فوراً وارفض.

قواعد صارمة:
- اسأل سؤالاً واحداً فقط في كل رسالة، لا تجمع أسئلة أبداً
- إذا كان الجواب غامضاً اطلب توضيحاً قبل الانتقال للسؤال التالي
- كن ودياً ومشجعاً، اعترف بكل إجابة باختصار قبل السؤال التالي
- لا تتجاوز الأسئلة الهامة حتى لو أجاب العميل بشكل عام
- إذا أجاب العميل في أي رسالة (سواء الأولى أو لاحقة) على سؤال أو أكثر من الأسئلة التي لم تُطرح عليه بعد، اعتمد تلك الإجابات فوراً واستخدمها مباشرة، ولا تعد السؤال عنها لاحقاً مهما كانت المرحلة — انتقل مباشرة للسؤال التالي الذي لم تُعرف إجابته بعد. هذه القاعدة تُطبَّق طوال المحادثة كاملة وليس فقط عند أول رسالة من العميل.
- راقب باستمرار كل رسالة من العميل (وليس فقط الرد على السؤال المطروح حالياً) بحثاً عن أي معلومات تجيب ضمنياً عن أسئلة لاحقة لم تصلها بعد، وسجّلها واستخدمها دون إعادة طرحها عند الوصول لدورها الطبيعي في التسلسل.
- إذا أرسل العميل في أي وقت ملخصاً جاهزاً وشاملاً عن مشروعه (يغطي معظم أو كل النقاط المطلوبة من الهوية والوظائف والتصميم وغيرها) بدلاً من الإجابة سؤالاً بسؤال، لا تتجاهل ذلك ولا تطلب منه إعادة الإجابة على الأسئلة بشكل منفصل. استخرج منه كل المعلومات المتاحة، واسأل فقط عن النقاط الناقصة الضرورية التي لم يغطها الملخص (سؤالاً واحداً في كل مرة كالمعتاد)، ثم انتقل مباشرة لإرسال ملخص الطلب النهائي دون تكرار ما أجاب عنه أصلاً.
- بعد جمع كل المعلومات، أرسل ملخصاً تفصيلياً شاملاً ومرتباً بالشكل التالي بالضبط:

╔══════════════════════════════════════╗
║       📋 ملف الطلب الكامل            ║
╚══════════════════════════════════════╝

🏷️ اسم المشروع: [اسم المشروع]
📂 نوع الخدمة: [التصنيف]
👤 اسم العميل: [الاسم]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 الهدف والرؤية:
[اكتب فقرة كاملة تشرح هدف المشروع ورؤيته ولماذا هو مهم وما المشكلة التي يحلها بتفصيل واسع]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👥 الجمهور المستهدف:
[اشرح بالتفصيل من هم المستخدمون المستهدفون، فئتهم العمرية، اهتماماتهم، واحتياجاتهم]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ الوظائف والميزات الأساسية:
[اشرح كل وظيفة وميزة بتفصيل كامل، كيف تعمل، وما الذي يراه المستخدم]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🗂️ هيكل الصفحات والأقسام:
[اذكر كل صفحة أو قسم بالتفصيل مع وصف محتواه ووظيفته]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔐 نظام الحسابات والصلاحيات:
[اشرح هل يوجد تسجيل دخول، وما أنواع المستخدمين، وما صلاحيات كل منهم]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🎨 الهوية البصرية والتصميم:
[اشرح الألوان المطلوبة، الأسلوب البصري، التطبيقات المشابهة، واللغات المدعومة]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

🔍 ميزات إضافية:
[اذكر البحث، الدردشة، أي ميزات أخرى ذكرها العميل بالتفصيل]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📝 ملاحظات وتعليمات خاصة من العميل:
[أي تفاصيل أو طلبات خاصة ذكرها العميل]

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

- اجعل الملخص طويلاً وغنياً بالتفاصيل، لا تختصر أي نقطة
- استخدم الإيموجي بشكل جميل ومنظم
- في نهاية الملخص أضف هذا النص حرفياً في سطر مستقل: [READY_TO_SEND]
- أجب دائماً بالعربية فقط`;
};

// ── نموذج Cerebras المستخدم في بوت الطلبات ──
const REQ_GEMINI_MODEL = CEREBRAS_MODEL; // يستخدم Cerebras الآن

// ══════════════════════════════════════════════════════════════════════
// ═══ تصنيف "إنشاء بوتات تلغرام": رفع الملفات لقناة التخزين الدائمة ═══
// نفس منطق /tg-upload الموجود، لكن كدالة داخلية تُستدعى من كود البوت
// مباشرة (بدون HTTP)، وتأخذ file_id من بوت الطلبات نفسه بدل base64.
// ══════════════════════════════════════════════════════════════════════
async function reqUploadFileToStorage(sourceBotApi, sourceFileId, mimeGuess, filenameGuess, targetBotApi, targetChatId) {
  try {
    // 1) جلب مسار الملف من البوت المصدر (بوت الطلبات)
    const fileInfoRes = await fetch(`${sourceBotApi}/getFile?file_id=${encodeURIComponent(sourceFileId)}`);
    const fileInfo    = await fileInfoRes.json();
    if (!fileInfo.ok || !fileInfo.result.file_path) throw new Error('تعذر جلب مسار الملف');

    const sourceToken = sourceBotApi.split('/bot')[1];
    const fileUrl  = `https://api.telegram.org/file/bot${sourceToken}/${fileInfo.result.file_path}`;
    const fileRes  = await fetch(fileUrl);
    if (!fileRes.ok) throw new Error('فشل تنزيل الملف من تيليجرام');
    const buffer   = Buffer.from(await fileRes.arrayBuffer());

    // 2) رفعه لقناة التخزين الدائمة (نفس منطق /tg-upload بالضبط)
    const mime = mimeGuess || 'application/octet-stream';
    const isPhoto = mime.startsWith('image/') && !mime.includes('gif');
    const isPDF   = mime === 'application/pdf';
    let endpoint, fieldName;
    if (isPhoto) { endpoint = '/sendPhoto'; fieldName = 'photo'; }
    else         { endpoint = '/sendDocument'; fieldName = 'document'; }

    const fname = filenameGuess || ('file_' + Date.now() + (isPhoto ? '.jpg' : isPDF ? '.pdf' : '.bin'));
    const boundary = '----ReqTgBotUpload' + Date.now().toString(36);
    const CRLF = '\r\n';
    const encodeField = (name, value) =>
      `--${boundary}${CRLF}Content-Disposition: form-data; name="${name}"${CRLF}${CRLF}${value}${CRLF}`;
    // ⚠️ مهم: يجب الرفع عبر البوت الذي سيستخدم الملف لاحقاً (uploadBotApi) إلى
    // شات يملكه/عضو فيه ذلك البوت بالذات (uploadChatId). السبب: الـ file_id
    // مرتبط بالبوت الذي رفع الملف تحديداً؛ إن رُفع عبر بوت آخر (بوت الطلبات
    // مثلاً أو بوت بلوتو) فلن يقدر البوت المُدار الجديد استخدام هذا الـ
    // file_id لاحقاً عند إرسال الملف للمستخدمين، فيفشل الجلب.
    // - الوضع الافتراضي (بدون targetBotApi): الرفع عبر بوت الطلبات لقناة
    //   "إنشاء مواقع وتطبيقات" (REQUEST_BACKUP_CHANNEL) — يُستخدم فقط كأرشيف
    //   عام قبل معرفة أي بوت مُدار سيستخدم الملف.
    // - إن مُرر targetBotApi/targetChatId: الرفع يتم عبر توكن البوت المُدار
    //   نفسه إلى مجموعة التخزين الخاصة به — هذا ما يضمن صلاحية الـ file_id.
    const uploadBotApi  = targetBotApi  || sourceBotApi;
    const uploadChatId  = targetChatId  || REQUEST_BACKUP_CHANNEL;
    let headerStr = encodeField('chat_id', uploadChatId);
    headerStr += `--${boundary}${CRLF}Content-Disposition: form-data; name="${fieldName}"; filename="${fname}"${CRLF}Content-Type: ${mime}${CRLF}${CRLF}`;
    const footer = `${CRLF}--${boundary}--${CRLF}`;
    const body = Buffer.concat([Buffer.from(headerStr, 'utf-8'), buffer, Buffer.from(footer, 'utf-8')]);

    const r = await fetch(uploadBotApi + endpoint, {
      method: 'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': body.length },
      body,
    });
    const data = await r.json();
    if (!data.ok) throw new Error(data.description || 'فشل الرفع لقناة التخزين');

    const resultMsg = data.result;
    let storedFileId = null;
    if (resultMsg.photo)    storedFileId = resultMsg.photo[resultMsg.photo.length - 1].file_id;
    else if (resultMsg.document) storedFileId = resultMsg.document.file_id;
    if (!storedFileId) throw new Error('لم يُعثر على file_id بعد الرفع');

    return { success: true, fileId: storedFileId, mime, filename: fname, isPhoto };
  } catch (e) {
    console.error('[ReqBot TgBuilder] خطأ رفع ملف:', e.message);
    return { success: false, error: e.message };
  }
}

// ── استخراج كتلة [BOT_DATA_JSON]...[/BOT_DATA_JSON] من رد الذكاء الاصطناعي ──
function reqExtractBotDataJson(aiReply) {
  try {
    const match = aiReply.match(/\[BOT_DATA_JSON\]([\s\S]*?)\[\/BOT_DATA_JSON\]/);
    if (!match) return null;
    return JSON.parse(match[1].trim());
  } catch (e) {
    console.error('[ReqBot TgBuilder] فشل تحليل BOT_DATA_JSON:', e.message);
    return null;
  }
}

// ── إزالة كتلة [BOT_DATA_JSON] من النص قبل عرضه للعميل ──
function reqStripBotDataJson(text) {
  return text.replace(/\[BOT_DATA_JSON\][\s\S]*?\[\/BOT_DATA_JSON\]/, '').trim();
}

// ══════════════════════════════════════════════════════════════════════
// ═══ تعليمات الذكاء الاصطناعي الخاصة بتعديل بوت تلغرام موجود ════════
// (تُعرض له الأزرار الحالية، ويتحاور مع العميل حول الإضافة/التعديل/الحذف،
//  ثم يُخرج نفس صيغة BOT_DATA_JSON الكاملة المُحدَّثة عند الجاهزية)
// ══════════════════════════════════════════════════════════════════════
function REQ_TG_BOT_EDIT_SYSTEM(currentButtons, botName) {
  return (
`أنت مساعد ذكي تحاور عميلاً يريد تعديل بوت تلغرام موجود مسبقاً باسم "${botName}".
هذه هي أزرار البوت الحالية بالضبط (بصيغة JSON):

${JSON.stringify(currentButtons, null, 2)}

مهمتك:
- استمع لطلب العميل: قد يريد إضافة زر جديد، تعديل زر موجود (اسمه أو محتواه أو نوعه)، حذف زر، أو تغيير ترتيب/توزيع الأزرار.
- اسأله بوضوح عن أي تفاصيل ناقصة (مثلاً: محتوى الزر الجديد، أو أي زر بالضبط يقصد إن كان الاسم غامضاً).
- إن أرسل ملفاً (صورة/PDF/APK) لزر جديد أو لاستبدال ملف زر موجود، اعتبره مرفقاً بأحدث سؤال متعلق بالملفات.
- لا تخترع أو تفترض أي شيء لم يذكره العميل صراحة.
- ⚠️ مهم جداً بخصوص الأزرار من نوع photo/pdf/apk التي لم يطلب العميل تغيير ملفها: يجب نسخ حقل "id" و"label" لها بالضبط وحرفياً كما وردا في القائمة الحالية أعلاه (بدون أي تعديل ولو بسيط بالمسافات أو الأحرف)، لأن ملفها القديم يُعاد ربطه تلقائياً بالمطابقة على "id" أو "label"، وأي اختلاف طفيف سيفقد الزر ملفه المرفق. اجعل "fileRef" لهذه الأزرار "null" دائماً.
- عندما تشعر أن كل التعديلات المطلوبة اكتملت ووضحت بالكامل، اسأله تأكيداً أخيراً: "هل هذه كل التعديلات المطلوبة؟ سأقوم بإعادة بناء البوت بها الآن." وعند تأكيده الإيجابي فقط، أخرج القائمة الكاملة المُحدَّثة لكل أزرار البوت (القديمة التي لم تتغير + المعدَّلة + الجديدة، وبدون المحذوفة) بالضبط بصيغة JSON التالية في نهاية ردك:

[BOT_DATA_JSON]
{
  "buttons": [
    {
      "id": "معرّف الزر القديم كما هو بالضبط إن كان الزر موجوداً مسبقاً ولم يُحذف، أو اتركه فارغاً/null إن كان زراً جديداً",
      "label": "اسم الزر",
      "type": "text أو link أو photo أو pdf أو apk",
      "content": "النص أو الرابط إن كان type=text أو link، وإلا اتركه فارغاً",
      "fileRef": "رقم ترتيب الملف الجديد الذي أرسله العميل ضمن هذه المحادثة بدءاً من 1، أو null إن كان الزر يستخدم ملفه القديم أو لا يحتاج ملفاً",
      "row": رقم الصف بدءاً من 1,
      "rowWidth": عدد الأزرار بنفس الصف (1 أو 2 أو 3)
    }
  ]
}
[/BOT_DATA_JSON]

- بعد كتلة JSON مباشرة أضف رسالة قصيرة ودودة تخبره أن التعديلات جاهزة لإعادة البناء.
- في نهاية الرسالة كاملة أضف هذا النص حرفياً في سطر مستقل: [READY_TO_SEND]
- طالما لم يكتمل الحوار أو لم يؤكد العميل، لا تُخرج كتلة JSON إطلاقاً، فقط أكمل الحوار بشكل طبيعي.
- أجب دائماً بالعربية فقط، وبأسلوب ودّي ومباشر.`
  );
}

// ══════════════════════════════════════════════════════════════════════
// ═══ تصنيف "إنشاء بوتات تلغرام": البناء التلقائي الكامل ═════════════
// البوتات المُنشأة تُخزَّن داخل requestAppData.managedBots (يُحفظ ويُسترجع
// تلقائياً بنفس آلية النسخ الاحتياطي الموجودة أصلاً لبوت الطلبات)
// { botId: { token, username, displayMode, buttons: [...], ownerChatId, createdAt } }
// ══════════════════════════════════════════════════════════════════════
const REQ_MANAGED_BOT_WEBHOOK_BASE = 'https://mahmoud08808665888888m-my-bot.hf.space/managed-bot';

// ── كاش بالذاكرة لإعدادات البوتات المُدارة (لتفادي قراءة/معالجة زائدة) ──
const reqManagedBotCache = {}; // { botId: configObject }

function reqGetManagedBotConfig(botId) {
  if (reqManagedBotCache[botId]) return reqManagedBotCache[botId];
  const cfg = (requestAppData.managedBots || {})[botId];
  if (cfg) reqManagedBotCache[botId] = cfg;
  return cfg || null;
}

// ── بناء reply_markup (inline أو keyboard) من مصفوفة الأزرار مع مراعاة row/rowWidth ──
function reqBuildManagedBotButtonsMarkup(buttons, displayMode) {
  // تجميع الأزرار حسب رقم الصف الذي حدده الذكاء الاصطناعي، مع الحفاظ على ترتيبها
  const rowsMap = {};
  buttons.forEach((btn, idx) => {
    const rowNum = Number.isInteger(btn.row) && btn.row > 0 ? btn.row : (idx + 1);
    if (!rowsMap[rowNum]) rowsMap[rowNum] = [];
    rowsMap[rowNum].push(btn);
  });
  const orderedRows = Object.keys(rowsMap).map(Number).sort((a, b) => a - b).map(k => rowsMap[k]);

  if (displayMode === 'keyboard') {
    const keyboard = orderedRows.map(row => row.map(btn => ({ text: btn.label })));
    return { reply_markup: { keyboard, resize_keyboard: true, is_persistent: true } };
  }
  // inline (افتراضي)
  const inline_keyboard = orderedRows.map(row => row.map(btn => {
    if (btn.type === 'link' && btn.content) return { text: btn.label, url: btn.content };
    return { text: btn.label, callback_data: `mbbtn_${btn.id}` };
  }));
  return { reply_markup: { inline_keyboard } };
}

// ── التحقق من صحة التوكن عبر getMe ──────────────────────────────────
// ── التحقق من أن البوت المُدار عضو/مشرف في مجموعة التخزين الخاصة به ──
async function reqVerifyBotInStorageGroup(token, groupId) {
  try {
    const meRes = await fetch(`https://api.telegram.org/bot${token}/getMe`);
    const meData = await meRes.json();
    if (!meData.ok) return { ok: false, error: 'توكن غير صالح' };
    const r = await fetch(`https://api.telegram.org/bot${token}/getChatMember?chat_id=${encodeURIComponent(groupId)}&user_id=${meData.result.id}`);
    const data = await r.json();
    if (!data.ok) return { ok: false, error: data.description || 'تعذر التحقق من عضوية البوت في المجموعة' };
    const status = data.result.status;
    if (status !== 'administrator' && status !== 'creator') {
      return { ok: false, error: `البوت عضو في المجموعة لكنه ليس مشرفاً (الحالة الحالية: ${status})` };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

async function reqVerifyBotToken(token) {
  try {
    const r = await fetch(`https://api.telegram.org/bot${token}/getMe`);
    const data = await r.json();
    if (!data.ok) return { valid: false, error: data.description || 'توكن غير صالح' };
    return { valid: true, username: data.result.username, botName: data.result.first_name };
  } catch (e) {
    return { valid: false, error: e.message };
  }
}

// ── تسجيل webhook تلقائي للبوت الجديد ────────────────────────────────
async function reqSetupManagedBotWebhook(token, botId) {
  const url = `${REQ_MANAGED_BOT_WEBHOOK_BASE}/${botId}`;
  const r = await fetch(`https://api.telegram.org/bot${token}/setWebhook?url=${encodeURIComponent(url)}`);
  const data = await r.json();
  if (!data.ok) throw new Error(data.description || 'فشل تسجيل webhook');
  await reqSetBotStartCommand(`https://api.telegram.org/bot${token}`);
  return true;
}

// ══ الدالة الرئيسية: بناء وتفعيل بوت تلغرام جديد تلقائياً من بيانات الجلسة ══
async function reqBuildTelegramBot(session, ownerChatInfo) {
  try {
    const botData = session.pendingBotData;
    if (!botData || !botData.token) {
      return { success: false, error: 'لم يتم العثور على بيانات صالحة للبوت (توكن مفقود). أعد إرسال طلبك.' };
    }

    // 1) التحقق من صلاحية التوكن فعلياً
    const verify = await reqVerifyBotToken(botData.token.trim());
    if (!verify.valid) {
      return { success: false, error: `التوكن غير صالح أو تم إبطاله: ${verify.error}` };
    }

    // 2) بناء قائمة الأزرار النهائية، مع ربط كل زر بالملف المناسب
    // ⚠️ إن وُجدت مجموعة تخزين خاصة بهذا البوت (session.tgBotStorageGroupId)،
    // يجب إعادة رفع كل ملف عبر توكن البوت الجديد نفسه إلى تلك المجموعة،
    // لأن الـ file_id الذي حصلنا عليه سابقاً عبر بوت الطلبات غير صالح لبوت آخر.
    const uploadedFiles = session.tgBotFiles || []; // [{ index, fileId, mime, filename, isPhoto, sourceFileId }]
    const oldButtons = Array.isArray(session.oldButtons) ? session.oldButtons : []; // الأزرار القديمة قبل التعديل (لإعادة استخدام ملفاتها إن لم تتغيّر)
    const newToken = botData.token.trim();
    const storageGroupId = session.tgBotStorageGroupId || null;

    const finalButtons = [];
    const buttonsArr = Array.isArray(botData.buttons) ? botData.buttons : [];
    // إن اختار العميل نمط توزيع ثابت عبر الأزرار (2/3/1 في كل صف)، نحسبه آلياً
    // بدقة رياضية بدل الاعتماد على تقدير الذكاء الاصطناعي لقيم row/rowWidth.
    const fixedLayout = Number.isInteger(session.tgLayoutPerRow)
      ? reqTgApplyLayoutPattern(buttonsArr.length, session.tgLayoutPerRow)
      : null;
    // نتتبع الأزرار القديمة المُستخدَمة بالفعل حتى لا يُعاد استخدام نفس الزر القديم مرتين
    // في حال تكرر نفس اسم الزر أكثر من مرة ضمن القائمة الجديدة
    const usedOldButtonIds = new Set();
    for (let i = 0; i < buttonsArr.length; i++) {
      const btn = buttonsArr[i];
      const built = {
        id:      `${Date.now().toString(36)}${i}`,
        label:   String(btn.label || `زر ${i + 1}`).slice(0, 60),
        type:    ['text', 'link', 'photo', 'pdf', 'apk'].includes(btn.type) ? btn.type : 'text',
        content: btn.content || '',
        row:     fixedLayout ? fixedLayout[i].row : (Number.isInteger(btn.row) ? btn.row : (i + 1)),
        rowWidth: fixedLayout ? fixedLayout[i].rowWidth : ([1, 2, 3].includes(btn.rowWidth) ? btn.rowWidth : 1),
        fileId:  null, fileMime: null
      };
      if (['photo', 'pdf', 'apk'].includes(built.type) && btn.fileRef) {
        // ── حالة 1: العميل أرسل ملفاً جديداً لهذا الزر ضمن محادثة التعديل ──
        const matchedFile = uploadedFiles.find(f => f.index === Number(btn.fileRef));
        if (matchedFile) {
          if (storageGroupId) {
            // إعادة رفع الملف عبر توكن البوت الجديد إلى مجموعته الخاصة
            const newBotApi = 'https://api.telegram.org/bot' + newToken;
            const reup = await reqUploadFileToStorage(
              REQUEST_MAIN_BOT_API, matchedFile.sourceFileId || matchedFile.fileId,
              matchedFile.mime, matchedFile.filename, newBotApi, storageGroupId
            );
            if (reup.success) { built.fileId = reup.fileId; built.fileMime = reup.mime; }
          } else {
            // لا توجد مجموعة تخزين خاصة (لم يطلب العميل ربط ملفات) — نستخدم القديم كحل احتياطي
            built.fileId = matchedFile.fileId; built.fileMime = matchedFile.mime;
          }
        }
      } else if (['photo', 'pdf', 'apk'].includes(built.type) && !btn.fileRef && oldButtons.length) {
        // ── حالة 2: الزر لم يتغيّر ملفه (fileRef فارغ) — نعيد استخدام ملفه القديم بالبحث
        // أولاً بنفس id إن وُجد، وإلا بنفس label ونفس type، وإلا بنفس الترتيب/النوع ──
        let match =
          (btn.id && oldButtons.find(ob => ob.id === btn.id && !usedOldButtonIds.has(ob.id))) ||
          oldButtons.find(ob => ob.label === built.label && ob.type === built.type && !usedOldButtonIds.has(ob.id));
        if (match && match.fileId) {
          usedOldButtonIds.add(match.id);
          built.fileId   = match.fileId;
          built.fileMime = match.fileMime;
        }
      }
      finalButtons.push(built);
    }

    if (finalButtons.length === 0) {
      return { success: false, error: 'لم يتم تحديد أي أزرار للبوت.' };
    }

    // 2.5) إن وُجدت مجموعة تخزين، تحقق أن البوت الجديد فعلاً مشرف فيها قبل المتابعة
    if (storageGroupId) {
      const groupCheck = await reqVerifyBotInStorageGroup(newToken, storageGroupId);
      if (!groupCheck.ok) {
        return { success: false, error: `لم يتم التحقق من ربط مجموعة التخزين: ${groupCheck.error}\n\nتأكد أنك أضفت البوت الجديد للمجموعة وجعلته مشرفاً، ثم أعد المحاولة.` };
      }
    }

    // 3) تسجيل webhook تلقائي على مسار عام موحّد لكل البوتات المُدارة
    const botId = verify.username; // معرف فريد ومستقر لكل بوت
    await reqSetupManagedBotWebhook(botData.token.trim(), botId);

    // 4) حفظ تعريف البوت (توكن + أزرار) بشكل دائم داخل requestAppData
    if (!requestAppData.managedBots) requestAppData.managedBots = {};
    const managedBotConfig = {
      botId,
      token:        botData.token.trim(),
      username:     verify.username,
      botName:      verify.botName,
      displayMode:  botData.displayMode === 'keyboard' ? 'keyboard' : 'inline',
      buttons:      finalButtons,
      welcomeText:  `👋 أهلاً بك في ${verify.botName || 'البوت'}!`,
      ownerChatId:  ownerChatInfo.id,
      ownerName:    `${ownerChatInfo.first_name || ''} ${ownerChatInfo.last_name || ''}`.trim(),
      extraNotes:   botData.extraNotes || '',
      storageGroupId: storageGroupId,
      createdAt:    new Date().toISOString()
    };
    requestAppData.managedBots[botId] = managedBotConfig;
    reqManagedBotCache[botId] = managedBotConfig;
    reqSaveLocalData();
    await reqBackupToChannel();

    return { success: true, username: verify.username, buttonCount: finalButtons.length };
  } catch (e) {
    console.error('[ReqBot TgBuilder] خطأ في بناء البوت:', e.message);
    return { success: false, error: e.message };
  }
}

// تقليص الـ messages للحفاظ على system + آخر N رسالة فقط
function reqTrimMessages(messages, maxPairs = 30) {
  if (messages.length <= 1) return messages;
  const system = messages[0]; // system message دائماً أول
  const rest   = messages.slice(1);
  // نحتفظ بآخر maxPairs*2 رسالة (user+assistant)
  const trimmed = rest.length > maxPairs * 2 ? rest.slice(-(maxPairs * 2)) : rest;
  return [system, ...trimmed];
}

// دالة استدعاء Gemini AI لبوت الطلبات — مع retry عند الخطأ
async function reqCallGroqAI(messages) {
  const trimmed = reqTrimMessages(messages, 30);
  const systemMsg = trimmed[0]?.role === 'system' ? trimmed[0].content : '';
  const chatMsgs  = trimmed[0]?.role === 'system' ? trimmed.slice(1) : trimmed;

  const cerebrasMessages = [
    ...(systemMsg ? [{ role: 'system', content: systemMsg }] : []),
    ...chatMsgs.map(m => ({ role: m.role === 'assistant' ? 'assistant' : 'user', content: m.content }))
  ];

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const r = await fetch(
        `${CEREBRAS_BASE_URL}/chat/completions`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${CEREBRAS_API_KEY}` },
          body: JSON.stringify({
            model: CEREBRAS_MODEL,
            messages: cerebrasMessages,
            max_completion_tokens: 2048,
            temperature: 0.7
          })
        }
      );

      // نقرأ الـ body كنص خام أولاً (مرة واحدة فقط) لنطبعه دائماً ونحلله بعدين
      const rawText = await r.text();
      console.log(`[ReqBot AI] 🔍 محاولة ${attempt}/3 — HTTP ${r.status} — body: ${rawText.slice(0, 500)}`);

      // rate limit → انتظر ثم أعد المحاولة
      if (r.status === 429) {
        const waitSec = attempt * 5;
        console.warn(`[ReqBot AI] rate-limit Cerebras — انتظار ${waitSec}ث (محاولة ${attempt}/3)`);
        await new Promise(res => setTimeout(res, waitSec * 1000));
        continue;
      }

      if (!r.ok) {
        console.error(`[ReqBot AI] ❌ Cerebras HTTP ${r.status} (محاولة ${attempt}) — ${rawText.slice(0, 500)}`);
        break;
      }

      let data;
      try {
        data = JSON.parse(rawText);
      } catch (parseErr) {
        console.error(`[ReqBot AI] ❌ رد Cerebras ليس JSON صالح: ${rawText.slice(0, 500)}`);
        break;
      }

      if (data.error) {
        console.warn(`[ReqBot AI] خطأ من Cerebras:`, data.error.message || JSON.stringify(data.error));
        break;
      }

      const content = data.choices?.[0]?.message?.content;
      if (content) return content;

      console.error(`[ReqBot AI] ⚠️ لا يوجد content بالرد مع أنه HTTP ${r.status} ناجح — الرد الكامل: ${rawText.slice(0, 500)}`);
      break;
    } catch(e) {
      console.error(`[ReqBot AI] استثناء Cerebras (محاولة ${attempt}):`, e.message, e.stack);
      if (attempt < 3) await new Promise(res => setTimeout(res, 2000));
    }
  }

  console.error('[ReqBot AI] ❌ فشل Cerebras');
  return null;
}

// ── بناء نص الطلب الكامل من كامل محادثة AI (لا يعتمد على [READY_TO_SEND]) ──
function reqBuildFullOrderText(session, chatInfo) {
  const chatId = chatInfo.id;

  // استخراج آخر رد من AI يحتوي على [READY_TO_SEND] — هو الملخص النهائي
  let lastAiSummary = '';
  for (let i = session.aiHistory.length - 1; i >= 0; i--) {
    const m = session.aiHistory[i];
    if (m.role === 'assistant' && m.content.includes('[READY_TO_SEND]')) {
      lastAiSummary = m.content.replace('[READY_TO_SEND]', '').trim();
      break;
    }
  }
  // إن لم يُوجد [READY_TO_SEND]، خذ آخر رد AI
  if (!lastAiSummary) {
    for (let i = session.aiHistory.length - 1; i >= 0; i--) {
      if (session.aiHistory[i].role === 'assistant') {
        lastAiSummary = session.aiHistory[i].content.replace('[READY_TO_SEND]', '').trim();
        break;
      }
    }
  }

  return lastAiSummary + '\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📌 للرد على العميل: chat_id = ' + chatId;
}

// ══════════════════════════════════════════════════════════════════════
// 🆕 قسم جديد: بناء index.html تلقائياً عبر Qwen3 Coder (OpenRouter)
// خاص فقط بتصنيفي "📱 إنشاء تطبيق APK" و"🌐 إنشاء موقع احترافي"
// لا يمس أي منطق قديم — الإرسال للقناة (reqSendOrderToChannel) يبقى
// يعمل تماماً كما هو، هذا القسم إضافة موازية فقط.
// ══════════════════════════════════════════════════════════════════════

// مفتاح OpenRouter — أضِفه في Variables and secrets على HuggingFace باسم:
//   QWEN_API_KEY
const QWEN_API_KEY   = process.env.QWEN_API_KEY;
const QWEN_API_URL   = 'https://openrouter.ai/api/v1/chat/completions';

// ─── قائمة الموديلات المجانية القوية بالبرمجة (بترتيب الأقوى للأضعف) ───
// تُستدعى جميعها بالتوازي بنفس اللحظة. أول رد ناجح يُعتمد فوراً ويتم
// إيقاف انتظار الباقي — إلا إذا ردّ أكثر من موديل بنفس اللحظة تقريباً،
// عندها يُفضَّل الأقوى (الأعلى ترتيباً بهذه القائمة).
const QWEN_MODELS = [
  'qwen/qwen3-coder:free',
  'openai/gpt-oss-120b:free',
  'deepseek/deepseek-r1:free',
  'meta-llama/llama-4-maverick:free',
  'nvidia/nemotron-3-ultra-550b-a55b:free',
  'z-ai/glm-4.5-air:free',
  'meta-llama/llama-3.3-70b-instruct:free',
  'google/gemini-2.0-flash-exp:free'
];

// ─── نظام طابور بسيط: حد أقصى طلبين متزامنين، والباقي ينتظر دوره ───
const QWEN_MAX_CONCURRENT = 2;
let   qwenActiveCount     = 0;
const qwenQueue           = []; // قائمة انتظار من الدوال (resolvers)

function qwenAcquireSlot() {
  return new Promise((resolve) => {
    if (qwenActiveCount < QWEN_MAX_CONCURRENT) {
      qwenActiveCount++;
      resolve();
    } else {
      qwenQueue.push(resolve);
    }
  });
}

function qwenReleaseSlot() {
  qwenActiveCount--;
  if (qwenQueue.length > 0) {
    const next = qwenQueue.shift();
    qwenActiveCount++;
    next();
  }
}

// هل يوجد طلب أو أكثر بانتظار دوره حالياً؟ (تُستخدم لإرسال رسالة "يوجد ضغط")
function qwenIsBusy() {
  return qwenActiveCount >= QWEN_MAX_CONCURRENT;
}

// ─── إرسال الطلب لكل الموديلات بالتوازي، واعتماد الأقوى عند التعادل ───
// عند فشل الجولة كاملة (كل الموديلات ردّت بخطأ/429)، ننتظر ثانية ونعيد
// الجولة من جديد على كل الموديلات، لحد سقف الوقت الإجمالي QWEN_RETRY_MAX_MS.
const QWEN_RETRY_INTERVAL_MS   = 1000;          // انتظار قبل إعادة جولة كاملة جديدة
const QWEN_RETRY_MAX_MS        = 10 * 60 * 1000; // 10 دقائق كحد أقصى إجمالاً لكل الطلب
const QWEN_TIEBREAK_WINDOW_MS  = 500;           // نافذة انتظار قصيرة لتفضيل موديل أقوى ردّ بنفس اللحظة تقريباً

function _sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

// ─── استدعاء موديل واحد مرة واحدة (بدون إعادة محاولة داخلية) ───
// يرجع { ok: true, content } عند النجاح، أو { ok: false, rateLimited } عند الفشل
async function _reqCallOneModel(model, fullPrompt) {
  try {
    const r = await fetch(QWEN_API_URL, {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': 'Bearer ' + QWEN_API_KEY
      },
      body: JSON.stringify({
        model: model,
        messages: [
          { role: 'user', content: fullPrompt }
        ]
      })
    });
    const data = await r.json();

    if (r.ok && data.choices && data.choices[0]) {
      return { ok: true, model, content: data.choices[0].message.content || null };
    }

    const isRateLimited = (r.status === 429) ||
      (data && data.error && data.error.code === 429);

    console.error(
      `[QwenCoder] ❌ [${model}] رد غير متوقع` +
      (isRateLimited ? ' (429 ازدحام المزود)' : '') + ':',
      JSON.stringify(data).slice(0, 300)
    );
    return { ok: false, model, rateLimited: isRateLimited };
  } catch (e) {
    console.error(`[QwenCoder] ❌ [${model}] خطأ في الاتصال:`, e.message);
    return { ok: false, model, rateLimited: false };
  }
}

// ─── جولة واحدة: استدعاء كل الموديلات بالتوازي، واعتماد الأقوى بين الناجحين ───
// تنتظر أول نجاح فوراً، ثم تمنح نافذة قصيرة (QWEN_TIEBREAK_WINDOW_MS) لإتاحة
// الفرصة لموديل أقوى (أعلى ترتيباً) كي يردّ بنفس اللحظة تقريباً قبل الحسم.
async function _reqRaceAllModels(fullPrompt) {
  const promises = QWEN_MODELS.map(model => _reqCallOneModel(model, fullPrompt));
  const results = new Array(QWEN_MODELS.length).fill(null);
  let settledCount = 0;

  return new Promise((resolve) => {
    let decided = false;
    let tiebreakTimer = null;

    function pickBestSoFar() {
      // نبحث ضمن الردود الواصلة لحد الآن عن أقوى موديل ناجح (الأقرب لبداية القائمة)
      for (let i = 0; i < QWEN_MODELS.length; i++) {
        if (results[i] && results[i].ok) return results[i];
      }
      return null;
    }

    function finalize() {
      if (decided) return;
      const best = pickBestSoFar();
      if (best) {
        decided = true;
        resolve({ ok: true, model: best.model, content: best.content });
      }
    }

    promises.forEach((p, idx) => {
      p.then((result) => {
        results[idx] = result;
        settledCount++;

        if (decided) return;

        if (result.ok) {
          // أول نجاح: امنح نافذة قصيرة لموديل أقوى محتمل قبل الحسم النهائي
          if (!tiebreakTimer) {
            tiebreakTimer = setTimeout(finalize, QWEN_TIEBREAK_WINDOW_MS);
          }
          // لو كان هذا أقوى موديل بالقائمة (index 0)، لا داعي للانتظار إطلاقاً
          if (idx === 0) {
            clearTimeout(tiebreakTimer);
            finalize();
          }
        }

        // إذا انتهت كل الطلبات ولم ينجح أي واحد منها
        if (settledCount === QWEN_MODELS.length && !decided) {
          if (tiebreakTimer) clearTimeout(tiebreakTimer);
          decided = true;
          resolve({ ok: false });
        }
      });
    });
  });
}

// ─── استدعاء الموديلات بالتوازي (Race) مع إعادة الجولة عند فشل الكل، وإرجاع الكود الناتج ───
// onFirstSuccess: دالة اختيارية تُستدعى مرة واحدة فقط عند أول رد ناجح مُعتمَد
async function reqCallQwenCoder(fullPrompt, onFirstSuccess) {
  if (!QWEN_API_KEY) {
    console.error('[QwenCoder] ❌ QWEN_API_KEY غير موجود في متغيرات البيئة');
    return null;
  }

  const startedAt = Date.now();
  let round = 0;

  while (true) {
    round++;
    const raceResult = await _reqRaceAllModels(fullPrompt);

    if (raceResult.ok) {
      console.log(`[QwenCoder] ✅ الموديل المعتمد: ${raceResult.model} (الجولة ${round})`);
      if (typeof onFirstSuccess === 'function') {
        try { await onFirstSuccess(); } catch (_) {}
      }
      return raceResult.content;
    }

    console.error(`[QwenCoder] 🔄 فشلت كل الموديلات بالجولة ${round} — إعادة المحاولة`);

    if (Date.now() - startedAt >= QWEN_RETRY_MAX_MS) {
      console.error(`[QwenCoder] ⛔ انتهت مهلة إعادة المحاولة (10 دقائق) بعد ${round} جولة — تم التوقف`);
      return null;
    }

    await _sleep(QWEN_RETRY_INTERVAL_MS);
  }
}

// ─── استخراج كود HTML الصافي من رد الذكاء الاصطناعي (إزالة أي شرح أو ```html فواصل) ───
function reqExtractHtmlFromAiReply(aiText) {
  if (!aiText) return null;
  // إن وُجدت كتلة كود ```html ... ``` أو ``` ... ```، خذ محتواها فقط
  const codeBlockMatch = aiText.match(/```(?:html)?\s*([\s\S]*?)```/i);
  if (codeBlockMatch && codeBlockMatch[1]) {
    return codeBlockMatch[1].trim();
  }
  // وإلا أرجع النص كاملاً كما هو (حال أرجع AI الكود مباشرة بدون تنسيق ```)
  return aiText.trim();
}

// ─── إرسال ملف index.html مباشرة لمحادثة المستخدم على تلغرام ───
async function reqSendIndexHtmlToUser(chatId, htmlContent, projectName) {
  try {
    const buffer   = Buffer.from(htmlContent, 'utf-8');
    const boundary = '----IndexHtmlDoc' + Date.now().toString(36);
    const CRLF     = '\r\n';
    const safeName = (projectName || 'مشروعك').replace(/[/\\?%*:|"<>]/g, '-').replace(/\s+/g, '_');
    const filename = `${safeName}.html`;
    const caption  = `✅ تم بناء ملف موقعك (${projectName || ''}) بنجاح!`;

    const hdr =
      `--${boundary}${CRLF}Content-Disposition: form-data; name="chat_id"${CRLF}${CRLF}${chatId}${CRLF}` +
      `--${boundary}${CRLF}Content-Disposition: form-data; name="caption"${CRLF}${CRLF}${caption}${CRLF}` +
      `--${boundary}${CRLF}Content-Disposition: form-data; name="document"; filename="${filename}"${CRLF}Content-Type: text/html; charset=utf-8${CRLF}${CRLF}`;
    const ftr  = `${CRLF}--${boundary}--${CRLF}`;
    const body = Buffer.concat([Buffer.from(hdr), buffer, Buffer.from(ftr)]);

    const r = await fetch(REQUEST_MAIN_BOT_API + '/sendDocument', {
      method:  'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': body.length },
      body
    });
    const data = await r.json();
    console.log('[QwenCoder] إرسال index.html للمستخدم:', data.ok ? '✅' : '❌', JSON.stringify(data).slice(0, 200));
    return data.ok;
  } catch (e) {
    console.error('[QwenCoder] ❌ خطأ في إرسال index.html للمستخدم:', e.message);
    return false;
  }
}

// ─── الدالة الرئيسية: تبني الموقع عبر Qwen3 Coder وترسله للمستخدم ───
// تُستدعى فقط لتصنيفي APK والموقع الاحترافي (المستدعي مسؤول عن هذا الشرط)
async function reqBuildWebsiteViaAiAndSend(fullPrompt, chatId, projectName) {
  // أعلم المستخدم فوراً إذا كان هناك ضغط قبل الدخول بالطابور
  if (qwenIsBusy()) {
    await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
      chat_id: chatId,
      text: '⏳ في ضغط كبير حالياً، الرجاء الانتظار وبنبدأ ببناء تطبيقك/موقعك بأقرب وقت.'
    });
  }

  await qwenAcquireSlot();
  try {
    await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
      chat_id: chatId,
      text: '🚀 جاري بناء موقعك الآن، برجاء الانتظار...'
    });

    // يُستدعى مرة واحدة فقط فور نجاح أول محاولة فعلية مع الموديل
    let firstSuccessNotified = false;
    const onFirstSuccess = async () => {
      if (firstSuccessNotified) return;
      firstSuccessNotified = true;
      await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
        chat_id: chatId,
        text: '🚀 لقد بدأ البناء الفعلي 📈'
      });
    };

    const aiReply = await reqCallQwenCoder(fullPrompt, onFirstSuccess);
    if (!aiReply) {
      await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
        chat_id: chatId,
        text: '😔 لم أستطع الرد. سيتم التواصل معك يدوياً من فريق الدعم قريباً.'
      });
      return false;
    }

    const htmlContent = reqExtractHtmlFromAiReply(aiReply);
    if (!htmlContent) {
      await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
        chat_id: chatId,
        text: '😔 تعذّر استخراج كود الموقع من رد الذكاء الاصطناعي. سيتم التواصل معك يدوياً من فريق الدعم قريباً.'
      });
      return false;
    }

    const sent = await reqSendIndexHtmlToUser(chatId, htmlContent, projectName);
    return sent;
  } finally {
    qwenReleaseSlot();
  }
}

// ── Part 1: رأس برومت تصنيف "موقع احترافي" (HTML/CSS/JS) — نفس السلوك السابق دون أي تغيير ──
function reqBuildWebsitePromptHeader() {
  return (
    `╔══════════════════════════════════════════════════════════════╗\n` +
    `║        🌐 طلب إنشاء تطبيق ويب متكامل — للمطور فقط          ║\n` +
    `╚══════════════════════════════════════════════════════════════╝\n\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
    `⚙️ تعليمات المطور الإلزامية — اقرأها بدقة قبل البدء:\n` +
    `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
    `📦 البنية والملفات:\n` +
    `- أنشئ تطبيق ويب متكامل في ملف HTML واحد فقط (كل CSS و JS داخله)\n` +
    `- استخدم HTML5 + CSS3 + Vanilla JavaScript نقية بدون أي مكتبات خارجية\n` +
    `- التطبيق يجب أن يكون Fully Responsive يعمل على جميع الأجهزة (جوال، تابلت، كمبيوتر)\n` +
    `- استخدم CSS Variables لتنظيم الألوان والقيم القابلة للتعديل\n` +
    `- أضف في <head> هذه الـ meta tags الإلزامية:\n` +
    `    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">\n` +
    `    <meta name="mobile-web-app-capable" content="yes">\n` +
    `    <meta name="apple-mobile-web-app-capable" content="yes">\n` +
    `    <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">\n` +
    `    <meta name="format-detection" content="telephone=no">\n` +
    `  وأضف في CSS هذه القواعد الإلزامية لمنع التكبير والنسخ وترجمة جوجل:\n` +
    `    * { -webkit-user-select: none; user-select: none; -webkit-touch-callout: none; }\n` +
    `    input, textarea { -webkit-user-select: text; user-select: text; }\n` +
    `    body { touch-action: pan-x pan-y; }\n` +
    `    .notranslate { translate: no; }\n` +
    `  وأضف في <html> الخاصية: translate="no" class="notranslate"\n\n` +
    `📱 الهيكل الإلزامي للتطبيق — يشبه تطبيق أندرويد وليس موقع ويب:\n` +
    `- لا يوجد scroll عمودي على المستوى الكلي — كل شيء يناسب الشاشة تماماً\n` +
    `- كل الصفحات تكون مرئية (display: none/block) لا يوجد تنقل حقيقي\n` +
    `- التطبيق يتكون من ثلاثة أجزاء رئيسية فقط:\n\n` +
    `  ① هيدر علوي (Top Header) — ثابت في الأعلى:\n` +
    `     • على اليمين: أيقونة التطبيق (صورة إن وُجدت وإلا أيقونة SVG مناسبة) + اسم التطبيق\n` +
    `     • على اليسار: أيقونة الإشعارات (جرس) مع badge للعدد إن وُجدت إشعارات\n` +
    `     • الخلفية: شفافة مع Glassmorphism (backdrop-filter: blur)\n` +
    `     • ارتفاع ثابت: 56px على الجوال، 64px على الكمبيوتر\n\n` +
    `  ② المحتوى الرئيسي (Main Content) — يملأ المساحة بين الهيدرَين:\n` +
    `     • overflow-y: auto داخل هذا القسم فقط إن احتاج\n` +
    `     • كل صفحات التطبيق تظهر هنا بالتناوب\n\n` +
    `  ③ هيدر سفلي / شريط التنقل (Bottom Navigation) — ثابت في الأسفل:\n` +
    `     • على اليمين: زر الصفحة الرئيسية (Home) مع أيقونة بيت\n` +
    `     • في الوسط/اليسار: الأقسام التي طلبها العميل (أيقونة + نص مختصر)\n` +
    `     • على أقصى اليسار: زر "حسابي" أو "الإعدادات" يحتوي على:\n` +
    `         - خيار اللغة (إن طُلب دعم متعدد اللغات)\n` +
    `         - خيار تسجيل الدخول / الخروج\n` +
    `     • عرض كل أيقونة: مساوٍ (flex: 1)\n` +
    `     • ارتفاع ثابت: 56px + padding للـ safe area في الجوال\n` +
    `     • الأيقونة النشطة تتميز بلون واضح\n\n` +
    `🔙 التحكم بزر الرجوع (Back Button) — إلزامي لسلوك تطبيق حقيقي وليس موقع ويب:\n` +
    `- ⚠️ استخدم History API (history.pushState) لتسجيل كل انتقال بين الصفحات/الأقسام (بما فيها فتح صفحة "حسابي" أو أي Modal/صفحة فرعية) كحالة جديدة في history.\n` +
    `- اعترض حدث popstate (زر الرجوع بالجهاز/المتصفح) بالمنطق التالي:\n` +
    `   • إن كان المستخدم بأي صفحة غير الرئيسية → لا يخرج من التطبيق، بل يرجع خطوة واحدة فقط للصفحة/الحالة السابقة (أو يغلق أي Modal مفتوح أولاً)\n` +
    `   • إن لم توجد حالة سابقة محفوظة → ينتقل للصفحة الرئيسية مباشرة (وليس الخروج)\n` +
    `   • فقط إذا كان المستخدم بالفعل في الصفحة الرئيسية يُسمح للسلوك الافتراضي بإغلاق التطبيق\n` +
    `- الهدف: أينما كان المستخدم، زر الرجوع يعيده خطوة بخطوة نحو الرئيسية أولاً، ولا يُغلق التطبيق إلا بعد الوصول فعلياً للرئيسية والضغط مرة إضافية.\n\n` +
    `🎨 التصميم البصري — يبدو كتطبيق أندرويد/iOS متقدم:\n` +
    `- الأزرار: زجاجية شفافة بتأثير Glassmorphism (backdrop-filter: blur + شفافية + حدود ناعمة)\n` +
    `- الألوان: تدرجات فاخرة Gradient ثابتة (لا متحركة) على الخلفية — اللون الثابت أسرع وأشبه بالتطبيقات\n` +
    `- الخلفية: ثابتة وغير متحركة (لا Animated Gradient ولا نجوم متحركة) إلا إذا طلب العميل ذلك صراحة\n` +
    `- الأيقونات: SVG مخصصة ومفصّلة لكل وظيفة، لا تستخدم emoji كأيقونات رئيسية\n` +
    `- الخطوط: استخدم Google Fonts (مسموح به) — اختر خطاً حديثاً يناسب طابع المشروع\n` +
    `- المظهر العام: Material Design أو Fluent Design — تطبيق وليس موقع ويب\n\n` +
    `✨ التأثيرات والحركة:\n` +
    `- انيميشن دخول للعناصر (fade-in + slide-up) عند تحميل الصفحة\n` +
    `- تأثير Hover على الأزرار والكروت (scale + glow)\n` +
    `- انتقالات سلسة بين الصفحات أو الأقسام (transitions)\n` +
    `- تأثير loading spinner أنيق عند الانتظار\n` +
    `- Scroll animations للعناصر عند التمرير\n\n` +
    `🧩 مكونات الواجهة:\n` +
    `- البطاقات (Cards): زجاجية مع ظل ناعم وحدود شبه شفافة\n` +
    `- النماذج (Forms): مدخلات أنيقة مع تأثير focus متوهج\n` +
    `- Toast Notifications: إشعارات جميلة تظهر وتختفي بسلاسة\n` +
    `- Modal/Dialog: نوافذ منبثقة زجاجية مع خلفية ضبابية\n` +
    `- جميع الأزرار في التطبيق يجب أن تكون وظيفية وتعمل فعلياً — لا يوجد زر بدون وظيفة\n` +
    `- لا توجد صفحات أو أقسام مخفية غير قابلة للوصول من شريط التنقل\n\n` +
    `🔽 القوائم المنسدلة / المنزلقات (Select / Dropdown) — ممنوع شكل المتصفح الافتراضي:\n` +
    `- ⚠️ ممنوع استخدام عنصر <select> الافتراضي للمتصفح كما هو بدون تنسيق — شكله الافتراضي (القائمة المنبثقة الرمادية البسيطة من الأسفل أو من نظام التشغيل/كروم) يكسر إحساس "التطبيق" فوراً ويبدو كموقع ويب عادي.\n` +
    `- ابنِ بديلاً مخصصاً (Custom Dropdown / Bottom Sheet) بـ HTML+CSS+JS خاص بالمشروع:\n` +
    `   • عند الضغط على الحقل، افتح قائمة منبثقة منسجمة مع باقي التصميم (نفس الزجاجية Glassmorphism، نفس الألوان والخط والحواف الدائرية المستخدمة بالتطبيق)\n` +
    `   • تظهر إما كـ Bottom Sheet ينزلق من الأسفل (الأنسب لتطبيقات الجوال) أو كقائمة منسدلة أنيقة أسفل الحقل مباشرة، مع انيميشن دخول/خروج ناعم (fade + slide)\n` +
    `   • كل خيار يكون عنصراً واضحاً قابلاً للمس بحجم مناسب للإصبع، مع تأثير عند الاختيار (تحديد واضح + علامة صح أو تظليل)\n` +
    `   • لا تستخدم إطلاقاً عنصر <select> الأصلي ولا حتى كأساس مخفي (لا polyfill يعتمد على select الأصلي ليظهر شكله الافتراضي عند الفتح)\n` +
    `- ⚠️ تنبيه إلزامي لتفادي كسر العمل داخل تطبيق APK (WebView) والموقع معاً عند بناء هذه القوائم المخصصة:\n` +
    `   • لا تعتمد على أي مكتبة خارجية أو CDN غير محمّل أصلاً بالصفحة — استخدم Vanilla JS فقط مثل باقي التطبيق\n` +
    `   • تأكد أن أحداث اللمس (touchstart/click) تعمل بشكل صحيح داخل WebView وليس فقط بمتصفح الكمبيوتر (تجنب الاعتماد فقط على hover أو على أحداث خاصة بالماوس)\n` +
    `   • اجعل القائمة المنبثقة فوق كل العناصر (z-index عالٍ بما يكفي) ولا تتعارض مع الهيدر العلوي أو السفلي الثابتين\n` +
    `   • أغلق القائمة تلقائياً عند اختيار عنصر أو عند الضغط خارجها، وتأكد من عدم بقاء أي طبقة شفافة تمنع الضغط على باقي عناصر الصفحة بعد إغلاقها (يجب إزالة أو إخفاء الـ overlay فعلياً من الـ DOM/الـ display، لا الاكتفاء بإخفائه بصرياً فقط)\n` +
    `   • اختبر منطقياً أن القيمة المختارة تُحفظ بشكل صحيح في متغير/state الصفحة وتُستخدم بنفس الطريقة التي كان سيُستخدم بها select عادي (لتفادي كسر أي منطق JS أو حفظ بيانات لاحق يعتمد على القيمة المختارة)\n\n` +
    `🖼️ الأيقونة:\n` +
    `- إذا أرسل العميل أيقونة (صورة) يجب استخدام رابطها مباشرة في:\n` +
    `    <link rel="icon" href="ICON_URL_HERE">\n` +
    `    <link rel="apple-touch-icon" href="ICON_URL_HERE">\n` +
    `  واستخدامها أيضاً في الهيدر العلوي كصورة دائرية\n` +
    `- إذا لم تُرسَل أيقونة استخدم SVG مناسب للمشروع\n\n` +
    `🌐 متعدد اللغات:\n` +
    `- إذا طلب العميل دعم أكثر من لغة، ضع خيار اللغة في قائمة "حسابي" بالهيدر السفلي\n` +
    `- استخدم data attributes لتخزين النصوص بكل لغة وقم بتبديلها عبر JavaScript\n\n` +
    `🔧 الوظائف التقنية — التخزين السحابي إلزامي (ممنوع منعاً باتاً استخدام localStorage):\n` +
    `- ⚠️ ممنوع تخزين أي بيانات بـ localStorage أو sessionStorage أو IndexedDB محلياً. كل بيانات المستخدمين (تسجيل، نماذج، مدفوعات، أي مدخلات) يجب أن تُخزَّن فقط على Firebase Firestore السحابي عبر الكود التالي:\n\n` +
    `  1) أضف داخل <head> أو قبل </body> مكتبات Firebase عبر CDN (compat، الأسهل للدمج بملف واحد):\n` +
    `     <script src="https://www.gstatic.com/firebasejs/10.13.0/firebase-app-compat.js"></script>\n` +
    `     <script src="https://www.gstatic.com/firebasejs/10.13.0/firebase-auth-compat.js"></script>\n` +
    `     <script src="https://www.gstatic.com/firebasejs/10.13.0/firebase-firestore-compat.js"></script>\n\n` +
    `  2) استخدم بالضبط إعدادات مشروع Firebase التالي (ثابتة، لا تُغيَّر مهما كان نوع الموقع):\n` +
    `     const firebaseConfig = {\n` +
    `       apiKey: "AIzaSyAm2KnkSP5D7r96IBewMwAGPPCxJy26WD4",\n` +
    `       authDomain: "temphostsites.firebaseapp.com",\n` +
    `       projectId: "temphostsites",\n` +
    `       storageBucket: "temphostsites.firebasestorage.app",\n` +
    `       messagingSenderId: "1068610733688",\n` +
    `       appId: "1:1068610733688:web:e09ca48be16bdda2a38f8d"\n` +
    `     };\n` +
    `     firebase.initializeApp(firebaseConfig);\n` +
    `     const auth = firebase.auth();\n` +
    `     const db   = firebase.firestore();\n\n` +
    `  3) ⚠️ مفتاح أساسي — مسار القاعدة الفريدة لهذا الموقع تحديداً:\n` +
    `     هذا الموقع سيُستضاف لاحقاً على رابط من الشكل: https://DOMAIN/p/SITE_SLUG\n` +
    `     يجب ألا تفترض قيمة SITE_SLUG مسبقاً، بل استخرجها ديناميكياً وقت تشغيل الصفحة من الرابط نفسه، عبر هذا الكود في بداية ملف الـ JS:\n` +
    `       const SITE_SLUG = window.location.pathname.split('/').filter(Boolean).pop();\n` +
    `     ثم استخدم هذا الثابت SITE_SLUG في كل مسارات Firestore بهذا الملف. يجب أن تخزَّن جميع بيانات هذا التطبيق فقط تحت المسار التالي (sub-collection باسم app_db تحت document الموقع)، ولا يجوز الكتابة في أي مسار آخر:\n` +
    `       temphost_sites/{SITE_SLUG}/app_db/{اسم مجموعة فرعية حسب نوع البيانات}/{معرف المستند}\n` +
    `     مثال عملي لتخزين بيانات مستخدم:\n` +
    `       db.collection('temphost_sites').doc(SITE_SLUG).collection('app_db').doc('users').collection('list').doc(uid).set({...})\n` +
    `     أو أي مجموعات فرعية أخرى حسب حاجة المشروع (orders, payments, submissions...)، كلها يجب أن تبدأ بنفس الجذر:\n` +
    `       db.collection('temphost_sites').doc(SITE_SLUG).collection('app_db')...\n\n` +
    `  4) تسجيل الدخول عبر Google — يجب أن يعمل في كل من المتصفح العادي وداخل تطبيقات APK المبنية من هذا الموقع (WebView). استخدم بالضبط هذا النمط المزدوج:\n\n` +
    `     أ) دالة موحّدة لزر تسجيل الدخول تكتشف البيئة تلقائياً:\n` +
    `     document.getElementById('googleSignInBtn').addEventListener('click', async () => {\n` +
    `       const isAndroidApp = !!(window.AndroidBridge && typeof window.AndroidBridge.isAndroidApp === 'function' && window.AndroidBridge.isAndroidApp());\n` +
    `       if (isAndroidApp) {\n` +
    `         // داخل تطبيق APK: استدعِ الجسر الأصلي، والنتيجة تصل لاحقاً عبر window.handleAndroidGoogleToken\n` +
    `         window.AndroidBridge.triggerGoogleSignIn();\n` +
    `         return;\n` +
    `       }\n` +
    `       // متصفح ويب عادي: تسجيل دخول قياسي\n` +
    `       const provider = new firebase.auth.GoogleAuthProvider();\n` +
    `       try { await auth.signInWithPopup(provider); }\n` +
    `       catch (err) { console.error(err); }\n` +
    `     });\n\n` +
    `     ب) دالتان عالميتان إلزاميتان لاستقبال نتيجة تسجيل الدخول من تطبيق APK (لا تحذفهما حتى لو لم يستخدمهما المتصفح العادي؛ تطبيق APK المبني من هذا الموقع يعتمد عليهما حصراً):\n` +
    `     window.handleAndroidGoogleToken = async function(idToken) {\n` +
    `       if (!idToken) return;\n` +
    `       try {\n` +
    `         const credential = firebase.auth.GoogleAuthProvider.credential(idToken);\n` +
    `         await auth.signInWithCredential(credential);\n` +
    `       } catch (err) { console.error(err); }\n` +
    `     };\n` +
    `     window.handleAndroidGoogleTokenError = function(message) {\n` +
    `       console.error('Google sign-in error:', message);\n` +
    `     };\n\n` +
    `     ج) بعد نجاح تسجيل الدخول (في أي من الحالتين)، استخدم onAuthStateChanged لحفظ بيانات المستخدم تحت نفس مسار الموقع:\n` +
    `     auth.onAuthStateChanged(user => {\n` +
    `       if (!user) return;\n` +
    `       db.collection('temphost_sites').doc(SITE_SLUG).collection('app_db').doc('users').collection('list').doc(user.uid)\n` +
    `         .set({ name: user.displayName || '', email: user.email || '', photo: user.photoURL || '' }, { merge: true });\n` +
    `     });\n\n` +
    `  5) كل عملية قراءة بيانات (مثل لوحة تحكم المدير لعرض المدفوعات أو المستخدمين) يجب أن تكون عبر استعلام Firestore (onSnapshot أو get()) من نفس مسار app_db الخاص بهذا الموقع، وليس من متغيرات JS محلية أو localStorage.\n` +
    `  5.1) ⚠️ لوحة تحكم المدير (إن وُجدت في الطلب) — قاعدة إلزامية: يُمنع أن تظهر لوحة التحكم كقسم أو تبويب عادي ضمن نفس واجهة المستخدم العادي (لا زر "لوحة تحكم" ظاهر للجميع في شريط التنقل أو القائمة). بدلاً من ذلك:\n` +
    `     - لوحة التحكم تكون مخفية تماماً عن واجهة المستخدم العادي ولا يُكشف عن وجودها.\n` +
    `     - تظهر فقط في حال قام المستخدم بتسجيل الدخول عبر Google بحساب بريد إلكتروني محدد مسبقاً (بريد المدير الذي يحدده العميل عند الطلب).\n` +
    `     - بعد نجاح تسجيل الدخول عبر Google، تحقق برمجياً إن كان user.email يطابق (بشكل حصري) بريد المدير المحدد؛ إن طابق، اعرض واجهة لوحة التحكم بدل واجهة المستخدم العادي (أو أضف لها مسار/زر يظهر فقط لهذا الحساب)، وإن لم يطابق فلا تُظهر أي أثر للوحة التحكم إطلاقاً.\n` +
    `     - لا تفترض بريد المدير من تلقاء نفسك؛ استخدم القيمة التي ترد ضمن تفاصيل طلب العميل (إن لم تُذكر، استخدم متغيراً واضحاً قابلاً للتعديل لاحقاً مثل ADMIN_EMAIL في أعلى ملف الجافاسكربت مع تعليق صريح يطلب من العميل لاحقاً تزويدك بالبريد الفعلي).\n\n` +
    `  6) لا داعٍ لجعل التطبيق يعمل Offline أو لتخزين أي نسخة احتياطية محلية؛ الاعتماد الكامل يكون على Firestore السحابي.\n` +
    `  7) تأكد من أداء سريع وكود نظيف ومنظم مع تعليقات واضحة\n` +
    `  8) اجعل الكود قابلاً للتوسع والتعديل بسهولة\n` +
    `  9) إذا أعطى العميل في أول رد له معلومات تغني عن أسئلة لم تُسأل بعد، استخدمها مباشرة ولا تسأله عنها مرة أخرى\n\n` +
    `🧱 منهجية البناء الإلزامية للملفات الكبيرة (أكثر من ~500 سطر متوقعة) — بناء بالتجزيء ثم الدمج:\n` +
    `بدل كتابة ملف HTML ضخم دفعة واحدة (صعب إدارته ومراقبة أخطاءه)، قسّم العمل لملفات منفصلة بحسب نوعها، وفي النهاية دمجها بسكربت بسيط:\n\n` +
    `  1) خطة الهيكل أولاً: أنشئ ملف index.html يحتوي فقط على هيكل الصفحة الأساسي (head، meta tags، روابط Firebase/الخطوط) مع 3 علامات نائبة (placeholders) بدل المحتوى الفعلي، مثل:\n` +
    `       <style>__CSS__</style>\n` +
    `       <body>__BODY__</body>\n` +
    `       <script>__JS__</script>\n\n` +
    `  2) كل نوع محتوى في ملف خاص به:\n` +
    `     - style.css ← كل الـ CSS لحاله\n` +
    `     - body.html ← كل الـ HTML (الصفحات، الكروت، الـ modals)\n` +
    `     - app1.js إلى app5.js (أو بحسب الحاجة) ← قسِّم الجافاسكربت حسب الوظيفة (مثلاً: إعدادات Firebase، المصادقة، عرض البيانات، المنطق الرئيسي، لوحة التحكم) بدل ملف JS واحد ضخم\n` +
    `     السبب: كل ملف صغير وواضح الغرض، يمكن مراجعته وتعديله لحاله بدون التوهان بملف آلاف الأسطر، ويمكن بناء الملفات الكبيرة على دفعات بدل طلب واحد طويل\n\n` +
    `  3) الدمج النهائي بسكربت Python بسيط: اقرأ index.html ثم استبدل كل placeholder بمحتوى ملفه المقابل (__CSS__ بمحتوى style.css، __BODY__ بمحتوى body.html، __JS__ بدمج app1.js...app5.js بالترتيب)، واكتب الناتج كملف HTML نهائي واحد\n\n` +
    `  4) فحص سلامة الملف قبل التسليم (إلزامي قبل اعتبار العمل منتهياً):\n` +
    `     - عدّ <div> مقابل </div> ويجب أن يتساويا\n` +
    `     - عدّ { مقابل } و( مقابل ) في الجافاسكربت ويجب أن يتساويا\n` +
    `     - تأكد من عدم تبقّي أي __CSS__ أو __BODY__ أو __JS__ بالملف النهائي (يعني الاستبدال صار صحيحاً بالكامل)\n\n` +
    `  5) نقل الملف النهائي المُدمَج فقط (لا الملفات الجزئية) كتسليم نهائي\n\n` +
    `  هذه المنهجية اختيارية لصفحات بسيطة وقصيرة، لكنها إلزامية لأي صفحة يُتوقع أن يتجاوز كودها الكامل (HTML+CSS+JS مجتمعة) حوالي 500 سطر، لتفادي تشتت الأخطاء وتسهيل المراجعة والتعديل اللاحق.\n\n`
  );
}

// ── إرسال الطلب الكامل للمجموعة كـ document نصي (يتجاوز حد 4096 حرف) ──
async function reqSendOrderToChannel(session, chatInfo) {
  try {
    const fullText = reqBuildFullOrderText(session, chatInfo);

    // ── بناء برومت احترافي للملف — تم حذف برومت بناء تطبيق Kotlin/APK نهائياً، وأصبح برومت الموقع (إنشاء موقع/تطبيق ويب) يُستخدم دائماً لكل التصنيفات بدون استثناء ──
    const headerBlock = reqBuildWebsitePromptHeader();

    const aiPrompt = (
      headerBlock +
      `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
      `📋 تفاصيل طلب العميل الكاملة:\n` +
      `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
      fullText
    );

    const buffer   = Buffer.from(aiPrompt, 'utf-8');
    const boundary = '----OrderDoc' + Date.now().toString(36);
    const CRLF     = '\r\n';
    const now      = new Date();
    const name     = chatInfo.first_name || 'عميل';
    const lastName = chatInfo.last_name || '';

    // اسم الملف = اسم التصنيف - اسم الشخص
    const safeCat    = (session.categoryLabel || 'طلب').replace(/[/\\?%*:|"<>]/g, '-').replace(/\s+/g, '_');
    const safeName   = (`${name} ${lastName}`).trim().replace(/[/\\?%*:|"<>]/g, '-').replace(/\s+/g, '_');
    const filename   = `${safeCat}-${safeName}.txt`;

    const caption  = (
      `📩 طلب جديد — ${session.categoryLabel}\n` +
      `👤 ${name} ${lastName} ${chatInfo.username ? '@' + chatInfo.username : ''}\n` +
      `🆔 ${chatInfo.id} | 📅 ${now.toLocaleDateString('ar-SA')}`
    );

    const hdr =
      `--${boundary}${CRLF}Content-Disposition: form-data; name="chat_id"${CRLF}${CRLF}${REQUEST_BACKUP_CHANNEL}${CRLF}` +
      `--${boundary}${CRLF}Content-Disposition: form-data; name="caption"${CRLF}${CRLF}${caption}${CRLF}` +
      `--${boundary}${CRLF}Content-Disposition: form-data; name="document"; filename="${filename}"${CRLF}Content-Type: text/plain; charset=utf-8${CRLF}${CRLF}`;
    const ftr  = `${CRLF}--${boundary}--${CRLF}`;
    const body = Buffer.concat([Buffer.from(hdr), buffer, Buffer.from(ftr)]);

    const r    = await fetch(REQUEST_MAIN_BOT_API + '/sendDocument', {
      method:  'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': body.length },
      body
    });
    const data = await r.json();
    console.log('[ReqBot] إرسال الطلب للمجموعة (بوت الطلبات):', data.ok ? '✅' : '❌', JSON.stringify(data).slice(0, 200));

    // حفظ message_id للرجوع إليه لاحقاً (نسخة بوت الطلبات)
    const orderMsgId = data.ok ? data.result.message_id : null;

    // ⚠️ رفع مكرر عبر بوت الأدمن أيضاً لنفس القناة — ضروري لأن forwardMessage
    // يتطلب أن يكون البوت المُنفِّذ عضواً في القناة المصدر لرسالة أرسلها هو نفسه
    // (أو أن يكون البوتان كلاهما مشرفين فيها)، وإرسال الرسالة عبر بوت واحد فقط
    // يمنع البوت الآخر أحياناً من جلبها بموثوقية. لذلك نرفع نسخة مستقلة بكل بوت.
    let orderMsgIdAdmin = null;
    const bodyAdmin = Buffer.concat([Buffer.from(hdr), buffer, Buffer.from(ftr)]);
    const rAdmin = await fetch(REQUEST_ADMIN_BOT_API + '/sendDocument', {
      method:  'POST',
      headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}`, 'Content-Length': bodyAdmin.length },
      body: bodyAdmin
    }).catch(e => { console.error('[ReqBot] فشل رفع نسخة بوت الأدمن للطلب:', e.message); return null; });
    if (rAdmin) {
      const dataAdmin = await rAdmin.json().catch(() => ({ ok: false }));
      console.log('[ReqBot] إرسال الطلب للمجموعة (بوت الأدمن):', dataAdmin.ok ? '✅' : '❌');
      if (dataAdmin.ok) orderMsgIdAdmin = dataAdmin.result.message_id;
    }

    // إرسال الصورة إن وُجدت (عبر البوتين أيضاً لنفس السبب)
    let photoMsgId = null;
    let photoMsgIdAdmin = null;
    if (session.photoFileId) {
      const pr = await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendPhoto', {
        chat_id: REQUEST_BACKUP_CHANNEL,
        photo:   session.photoFileId,
        caption: `🖼 أيقونة طلب: ${session.categoryLabel} | 🆔 ${chatInfo.id}`
      }).catch(e => { console.error('[ReqBot] فشل إرسال الصورة (بوت الطلبات):', e.message); return {}; });
      if (pr.ok) photoMsgId = pr.result.message_id;

      // نسخة الصورة عبر بوت الأدمن: نجلب الملف عبر بوت الطلبات ثم نرفعه من جديد عبر بوت الأدمن
      // (لأن file_id الصورة مرتبط ببوت الطلبات فقط ولا يعمل مباشرة مع بوت الأدمن)
      const reup = await reqUploadFileToStorage(
        REQUEST_MAIN_BOT_API, session.photoFileId, 'image/jpeg', null,
        REQUEST_ADMIN_BOT_API, REQUEST_BACKUP_CHANNEL
      ).catch(() => null);
      if (reup && reup.success) {
        const prAdmin = await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendPhoto', {
          chat_id: REQUEST_BACKUP_CHANNEL,
          photo:   reup.fileId,
          caption: `🖼 أيقونة طلب: ${session.categoryLabel} | 🆔 ${chatInfo.id}`
        }).catch(() => ({}));
        if (prAdmin.ok) photoMsgIdAdmin = prAdmin.result.message_id;
      }
    }

    return { orderMsgId, photoMsgId, orderMsgIdAdmin, photoMsgIdAdmin };
  } catch(e) {
    console.error('[ReqBot] خطأ في إرسال الطلب للمجموعة:', e.message);
    return { orderMsgId: null, photoMsgId: null, orderMsgIdAdmin: null, photoMsgIdAdmin: null };
  }
}

// ── حفظ بيانات الطلب (message_id في المجموعة) في requestAppData ──
function reqSaveOrder(session, chatInfo, orderMsgId, photoMsgId, orderMsgIdAdmin, photoMsgIdAdmin) {
  if (!requestAppData.orders) requestAppData.orders = [];
  const now = new Date();
  const order = {
    id:            Date.now(),
    date:          now.toISOString(),
    dateStr:       now.toLocaleDateString('ar-SA', { year: 'numeric', month: 'long', day: 'numeric' }),
    timeStr:       now.toLocaleTimeString('ar-SA', { hour: '2-digit', minute: '2-digit' }),
    categoryId:    session.categoryId,
    categoryLabel: session.categoryLabel,
    categoryKind:  session.categoryId === REQ_CAT_TG_BOT_ID ? 'bot' : (session.categoryKind || 'website'),
    clientName:    `${chatInfo.first_name || ''} ${chatInfo.last_name || ''}`.trim() || 'عميل',
    clientUsername: chatInfo.username ? `@${chatInfo.username}` : '',
    chatId:        chatInfo.id,
    hasPhoto:      !!session.photoFileId,
    photoFileId:   session.photoFileId || null,
    historyCount:  session.aiHistory.filter(m => m.role === 'user').length,
    channelMsgId:      orderMsgId       || null,  // نسخة بوت الطلبات (main)
    photoMsgId:        photoMsgId       || null,  // نسخة بوت الطلبات (main)
    channelMsgIdAdmin: orderMsgIdAdmin  || null,  // نسخة بوت الأدمن (admin)
    photoMsgIdAdmin:   photoMsgIdAdmin  || null,  // نسخة بوت الأدمن (admin)
    confirmed:     false   // يصبح true عندما يؤكده المدير
  };
  requestAppData.orders.unshift(order);
  if (requestAppData.orders.length > 200) requestAppData.orders = requestAppData.orders.slice(0, 200);
  reqSaveLocalData();
}

// ══════════════════════════════════════════════════════════════════════
// ═══ بوت الطلبات: webhook البوت الرئيسي ══════════════════════════════
// ══════════════════════════════════════════════════════════════════════
app.post('/request-bot-webhook/main', (req, res) => {
  res.sendStatus(200);
  setImmediate(async () => {
    try {
      const update = req.body;
      if (!update) return;

      // ─── callback_query ───────────────────────────────────────────
      if (update.callback_query) {
        const query  = update.callback_query;
        const chatId = query.message.chat.id;
        const data   = query.data;

        await reqSendToTg(REQUEST_MAIN_BOT_API, 'answerCallbackQuery', { callback_query_id: query.id });

        if (reqIsUserBlocked(chatId)) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⛔ تم حظرك من استخدام هذا البوت.' });
          return;
        }

        // ── زر إرسال الطلب النهائي ───────────────────────────────────
        if (data === 'confirm_request') {
          const session = reqAiSessions[chatId];
          if (!session) return;
          const chatInfo = query.message.chat;
          const { orderMsgId, photoMsgId, orderMsgIdAdmin, photoMsgIdAdmin } = await reqSendOrderToChannel(session, chatInfo);
          reqSaveOrder(session, chatInfo, orderMsgId, photoMsgId, orderMsgIdAdmin, photoMsgIdAdmin);
          const name  = chatInfo.first_name || 'عميل';
          const uname = chatInfo.username ? ` @${chatInfo.username}` : '';
          const notify = (
            `🔔 *طلب جديد وصل!*\n\n` +
            `🏷 النوع: ${session.categoryLabel}\n` +
            `👤 العميل: ${name} ${chatInfo.last_name || ''}${uname}\n` +
            `🆔 ID: ${chatInfo.id}\n` +
            `💬 عدد الردود: ${session.aiHistory.filter(m => m.role === 'user').length}\n` +
            `📎 صورة: ${session.photoFileId ? '✅' : '❌'}\n\n` +
            `📋 الطلب الكامل مُرسَل للمجموعة\n` +
            `اضغط "قائمة الطلبات" لعرضه.`
          );
          await reqNotifyAllAdmins({ text: notify, parse_mode: 'Markdown' });
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text: requestAppData.thank_you_text, ...reqBuildMainMenuKeyboard()
          });

          // 🆕 بناء الموقع تلقائياً عبر Qwen3 Coder — فقط لتصنيفي APK والموقع الاحترافي
          // (لا يشمل تصنيف "إنشاء بوتات تلغرام" إطلاقاً). يعمل بالتوازي دون
          // انتظار المستخدم، ولا يؤثر على مسار الإرسال للقناة أعلاه بأي شكل.
          if (session.categoryId === REQ_CAT_APK_ID || session.categoryId === REQ_CAT_SITE_ID) {
            const fullPromptForAi = (
              reqBuildWebsitePromptHeader() +
              `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n` +
              `📋 تفاصيل طلب العميل الكاملة:\n` +
              `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n` +
              reqBuildFullOrderText(session, chatInfo)
            );
            reqBuildWebsiteViaAiAndSend(fullPromptForAi, chatId, session.categoryLabel)
              .catch(e => console.error('[QwenCoder] ❌ خطأ غير متوقع في بناء الموقع:', e.message));
          }

          delete reqAiSessions[chatId];
          return;
        }

        if (data === 'cancel_request') {
          delete reqAiSessions[chatId];
          delete reqUserSessions[chatId];
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '❌ تم إلغاء الطلب.', ...reqBuildMainMenuKeyboard() });
          return;
        }

        // ── زر إعادة محاولة رسالة فشلت أثناء محادثة تعديل بوت بالذكاء الاصطناعي ──
        if (data.startsWith('projedit_retry_')) {
          const uSession = reqUserSessions[chatId];
          if (!uSession || uSession.action !== 'bot_edit_chat' || !uSession.editHistory?.length) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ انتهت الجلسة، ابدأ من جديد عبر قسم "مشاريعي".' });
            return;
          }
          const botCfg = reqGetManagedBotConfig(uSession.botId);
          if (!botCfg) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر العثور على البوت.' });
            delete reqUserSessions[chatId];
            return;
          }
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ إعادة المحاولة...' });
          const messages = [
            { role: 'system', content: REQ_TG_BOT_EDIT_SYSTEM(botCfg.buttons || [], botCfg.botName || botCfg.username) },
            ...uSession.editHistory
          ];
          const aiReply = await reqCallGroqAI(messages);
          if (!aiReply) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: '😔 ما زال هناك خطأ تقني في الاتصال بالذكاء الاصطناعي. حاول بعد دقائق قليلة.',
              reply_markup: { inline_keyboard: [[{ text: '🔁 إعادة المحاولة', callback_data: `projedit_retry_${uSession.projectId}` }]] }
            });
            return;
          }
          uSession.editHistory.push({ role: 'assistant', content: aiReply });
          const isReady   = aiReply.includes('[READY_TO_SEND]');
          const cleanReply = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanReply, parse_mode: 'Markdown' });
          if (isReady) {
            const updatedData = reqExtractBotDataJson(aiReply);
            if (updatedData && Array.isArray(updatedData.buttons)) {
              uSession.pendingEditButtons = updatedData.buttons;
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: chatId,
                text: '🚀 التعديلات جاهزة، اضغط الزر أدناه لإعادة بناء البوت بها الآن:',
                reply_markup: { inline_keyboard: [[{ text: '🔁 إعادة البناء', callback_data: 'bot_edit_rebuild' }]] }
              });
            } else {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ حدث خطأ تقني في تجهيز التعديلات، حاول وصف طلبك مجدداً.' });
            }
          }
          return;
        }

        if (data === 'back_to_menu') {
          delete reqAiSessions[chatId];
          delete reqUserSessions[chatId];
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: requestAppData.welcome_text, ...reqBuildMainMenuKeyboard() });
          return;
        }

        // ═══ تدفّق "تحويل مشروعي إلى دائم" ═══════════════════════

        // 1) اختيار نوع المشروع
        if (data.startsWith('permtype_')) {
          const type = data.replace('permtype_', ''); // apk | website | bot
          const projects = reqGetUserProjectsByType(chatId, type);
          if (!projects.length) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: `ℹ️ لا توجد لديك مشاريع مؤكدة من نوع "${reqPermanentTypeLabel(type)}" حالياً.`
            });
            delete reqPermanentSessions[chatId];
            return;
          }
          reqPermanentSessions[chatId] = { step: 'choose_project', type };
          const btns = projects.map(p => [{ text: `📦 ${p.name}`, callback_data: `permproj_${p.id}` }]);
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `📦 اختر المشروع الذي تريد تحويله إلى دائم من قسم "${reqPermanentTypeLabel(type)}":`,
            reply_markup: { inline_keyboard: btns }
          });
          return;
        }

        // 2) اختيار مشروع محدد → عرض رسالة تأكيد المدير أو رابط البوت + سؤال "هل هذا هو المشروع؟"
        if (data.startsWith('permproj_')) {
          const projectId = data.replace('permproj_', '');
          const found = reqFindProjectById(projectId);
          if (!found || String(found.ownerId) !== String(chatId)) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          reqPermanentSessions[chatId] = { step: 'confirm_project', project: found.project };
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `📦 *${found.project.name}*\n\n${reqProjectIdentityText(found.project)}\n\n❓ هل هذا هو المشروع الذي تريد تحويله إلى دائم؟`,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [
              [{ text: '✅ نعم', callback_data: 'permconfirm_yes' }, { text: '❌ لا', callback_data: 'permconfirm_no' }]
            ] }
          });
          return;
        }

        // 3) تأكيد "لا" → رجوع لاختيار مشروع آخر من نفس النوع
        if (data === 'permconfirm_no') {
          const pSession = reqPermanentSessions[chatId];
          if (!pSession || !pSession.project) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ ابدأ من جديد عبر زر "تحويل مشروعي إلى دائم".' });
            return;
          }
          const type = pSession.project.type === 'apk' ? 'apk' : (pSession.project.type === 'bot' ? 'bot' : 'website');
          const projects = reqGetUserProjectsByType(chatId, type);
          reqPermanentSessions[chatId] = { step: 'choose_project', type };
          const btns = projects.map(p => [{ text: `📦 ${p.name}`, callback_data: `permproj_${p.id}` }]);
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `📦 اختر المشروع الصحيح من قسم "${reqPermanentTypeLabel(type)}":`,
            reply_markup: { inline_keyboard: btns }
          });
          return;
        }

        // 4) تأكيد "نعم" → عرض خيارات الدفع
        if (data === 'permconfirm_yes') {
          const pSession = reqPermanentSessions[chatId];
          if (!pSession || !pSession.project) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ ابدأ من جديد عبر زر "تحويل مشروعي إلى دائم".' });
            return;
          }
          pSession.step = 'choose_payment';
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: '💳 اختر طريقة الدفع:',
            reply_markup: { inline_keyboard: [
              [{ text: '🟡 الدفع عبر Binance', callback_data: 'permpay_binance' }]
            ] }
          });
          return;
        }

        // 5) اختيار الدفع عبر بينانس → عرض السعر والسؤال "هل أنت جاهز للدفع؟"
        if (data === 'permpay_binance') {
          const pSession = reqPermanentSessions[chatId];
          if (!pSession || !pSession.project) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ ابدأ من جديد عبر زر "تحويل مشروعي إلى دائم".' });
            return;
          }
          const price = pSession.project.permanentPrice;
          if (!price) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: '⚠️ لم يحدّد المدير سعراً لهذا المشروع بعد. يرجى الانتظار حتى يتم تحديد السعر ثم إعادة المحاولة.'
            });
            return;
          }
          pSession.step = 'ready_confirm';
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `💰 سعر تحويل هذا المشروع إلى دائم هو: *${price}$*\n\n❓ هل أنت جاهز للدفع؟`,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [
              [{ text: '✅ نعم، جاهز', callback_data: 'permpay_ready_yes' }, { text: '❌ ليس الآن', callback_data: 'permpay_ready_no' }]
            ] }
          });
          return;
        }

        if (data === 'permpay_ready_no') {
          delete reqPermanentSessions[chatId];
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ حسناً، يمكنك العودة في أي وقت عبر زر "تحويل مشروعي إلى دائم".' });
          return;
        }

        // 6) جاهز للدفع → إرسال الصورة + الشبكة + العنوان + الرمز + زر "لقد قمت بالتحويل"
        if (data === 'permpay_ready_yes' || data === 'permpay_retry_txid') {
          const pSession = reqPermanentSessions[chatId];
          if (!pSession || !pSession.project) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ ابدأ من جديد عبر زر "تحويل مشروعي إلى دائم".' });
            return;
          }
          pSession.step = 'transferring';
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendPhoto', {
            chat_id: chatId,
            photo: REQ_PERMANENT_PAY_IMAGE_URL
          });
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: `الشبكة: ${REQ_PERMANENT_PAY_NETWORK}` });
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'عنوان الإيداع:' });
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `\`${REQ_PERMANENT_PAY_ADDRESS}\``,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '✅ لقد قمت بالتحويل', callback_data: 'permpay_done' }]] }
          });
          return;
        }

        // 7) الضغط على "لقد قمت بالتحويل" → طلب بيانات التحقق (TxID)
        if (data === 'permpay_done') {
          const pSession = reqPermanentSessions[chatId];
          if (!pSession || !pSession.project) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ ابدأ من جديد عبر زر "تحويل مشروعي إلى دائم".' });
            return;
          }
          pSession.step = 'await_txid';
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: (
              `🔎 لإتمام التحقق من عملية التحويل، أرسل الآن رمز/رقم العملية (Transaction ID / Hash) الخاص بعملية التحويل التي قمت بها.\n\n` +
              `يمكنك نسخه من تطبيق Binance من تفاصيل عملية السحب.`
            )
          });
          return;
        }

        // ── عرض تفاصيل مشروع محدد (من "مشاريعي") ──────────────────
        if (data.startsWith('myproj_')) {
          const projectId = data.replace('myproj_', '');
          const found = reqFindProjectById(projectId);
          if (!found || String(found.ownerId) !== String(chatId)) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          await reqSendProjectDetails(chatId, found.project);
          return;
        }

        // ── زر إعادة بناء بوت تلغرام بعد التعديل بالذكاء الاصطناعي ──
        if (data === 'bot_edit_rebuild') {
          const uSession = reqUserSessions[chatId];
          if (!uSession || uSession.action !== 'bot_edit_chat' || !uSession.pendingEditButtons) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ لا توجد تعديلات جاهزة، ابدأ من جديد عبر قسم "مشاريعي".' });
            return;
          }
          const botCfg = reqGetManagedBotConfig(uSession.botId);
          if (!botCfg) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر العثور على البوت.' });
            delete reqUserSessions[chatId];
            return;
          }

          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ إعادة بناء البوت بالتعديلات الجديدة...' });

          // إعادة بناء "جلسة" متوافقة مع reqBuildTelegramBot، بنفس التوكن والمجموعة القديمة
          const rebuildSession = {
            pendingBotData: {
              token:       botCfg.token,
              displayMode: botCfg.displayMode,
              buttons:     uSession.pendingEditButtons,
              extraNotes:  botCfg.extraNotes || ''
            },
            tgBotFiles:        uSession.editFiles || [],
            tgBotStorageGroupId: botCfg.storageGroupId || null,
            tgLayoutPerRow:    null, // نحافظ على row/rowWidth كما حدّدها الذكاء الاصطناعي بدل نمط ثابت
            oldButtons:        botCfg.buttons || [] // الأزرار القديمة (لإعادة استخدام ملفات الصور/PDF/APK غير المتغيّرة)
          };
          const ownerChatInfo = {
            id:         botCfg.ownerChatId,
            first_name: botCfg.ownerName || '',
            last_name:  ''
          };

          const rebuildResult = await reqBuildTelegramBot(rebuildSession, ownerChatInfo);
          delete reqUserSessions[chatId];

          if (rebuildResult.success) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: `🎉 تم إعادة بناء البوت بنجاح بـ ${rebuildResult.buttonCount} زر!\n\n🔗 https://t.me/${rebuildResult.username}`
            });
            await reqNotifyAllAdmins({
              text: `🔁 *تم تعديل بوت تلغرام*\n\n🤖 @${rebuildResult.username}\n👤 المالك: ${ownerChatInfo.first_name} (${ownerChatInfo.id})`,
              parse_mode: 'Markdown'
            });
          } else {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: `❌ فشلت إعادة البناء: ${rebuildResult.error}`
            });
          }
          return;
        }

        // ── زر تعديل مشروع: بدء جلسة AI لوصف التعديل بدقة ──────────
        if (data.startsWith('projedit_')) {
          const projectId = data.replace('projedit_', '');
          const found = reqFindProjectById(projectId);
          if (!found || String(found.ownerId) !== String(chatId)) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }

          // ── مشروع من نوع "بوت تلغرام": نفتح محادثة AI تفاعلية مباشرة ──
          if (found.project.type === 'bot') {
            const botCfg = reqFindManagedBotByUrl(found.project.url);
            if (!botCfg) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر العثور على إعدادات هذا البوت. تواصل مع الإدارة مباشرة.' });
              return;
            }
            reqUserSessions[chatId] = {
              action: 'bot_edit_chat',
              projectId,
              botId: botCfg.botId,
              editHistory: [],
              editFiles: []   // [{ index, fileId, mime, filename }] لأي ملفات جديدة تُرسل أثناء التعديل
            };
            // عرض الأزرار الحالية للبوت بشكل واضح للمستخدم قبل بدء الحوار
            const buttonsListText = (botCfg.buttons || []).map((b, i) =>
              `${i + 1}. ${b.label} — ${b.type === 'text' ? 'نص' : b.type === 'link' ? 'رابط' : b.type === 'photo' ? 'صورة' : b.type === 'pdf' ? 'PDF' : 'APK'}`
            ).join('\n') || '(لا توجد أزرار بعد)';
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: (
                `✏️ *تعديل بوت: ${botCfg.botName || botCfg.username}*\n\n` +
                `🔘 *الأزرار الحالية:*\n${buttonsListText}\n\n` +
                `أخبرني ما الذي تريد تعديله أو إضافته أو استبداله، وسأساعدك خطوة بخطوة.`
              ),
              parse_mode: 'Markdown'
            });
            return;
          }

          // ── مشروع موقع/تطبيق: يبقى وصف نصي يُرسل للإدارة كما هو ──
          reqUserSessions[chatId] = { action: 'project_edit_describe', projectId };
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `✏️ *تعديل: ${found.project.name}*\n\nصف لي بدقة ما الذي تريد تغييره في مشروعك (كل التفاصيل الممكنة تساعدنا ننفذها بشكل صحيح):`,
            parse_mode: 'Markdown'
          });
          return;
        }

        // ── زر حذف مشروع: طلب تأكيد ─────────────────────────────────
        if (data.startsWith('projdel_')) {
          const projectId = data.replace('projdel_', '');
          const found = reqFindProjectById(projectId);
          if (!found || String(found.ownerId) !== String(chatId)) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `🗑 هل أنت متأكد من حذف مشروع *${found.project.name}* من قائمة مشاريعك؟\n\n(هذا لا يحذف الموقع/البوت الفعلي، فقط يزيله من قائمتك هنا)`,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [
              [{ text: '✅ نعم، احذف', callback_data: `projdelconfirm_${projectId}` }],
              [{ text: '❌ إلغاء', callback_data: `myproj_${projectId}` }]
            ]}
          });
          return;
        }

        // ── تأكيد حذف المشروع ────────────────────────────────────────
        if (data.startsWith('projdelconfirm_')) {
          const projectId = data.replace('projdelconfirm_', '');
          const found = reqFindProjectById(projectId);
          if (!found || String(found.ownerId) !== String(chatId)) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          found.list.splice(found.list.indexOf(found.project), 1);
          reqSaveLocalData();
          await reqBackupToChannel();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم حذف المشروع من قائمتك.' });
          return;
        }

        // ── زر إيقاف/تشغيل المشروع (من طرف صاحب المشروع) ───────────
        if (data.startsWith('projtoggle_')) {
          const projectId = data.replace('projtoggle_', '');
          const found = reqFindProjectById(projectId);
          if (!found || String(found.ownerId) !== String(chatId)) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          if (found.project.adminStopped) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: '⛔ تم إيقاف هذا المشروع من قبل الإدارة، لا يمكنك تشغيله بنفسك. تواصل مع الإدارة عبر زر "جعل المشروع يعمل باستمرار".'
            });
            return;
          }
          found.project.active = found.project.active === false ? true : false;
          reqSaveLocalData();
          await reqBackupToChannel();
          await reqSendProjectDetails(chatId, found.project);
          return;
        }

        // ── زر جعل المشروع يعمل باستمرار ────────────────────────────
        if (data.startsWith('projkeepalive_')) {
          const projectId = data.replace('projkeepalive_', '');
          const found = reqFindProjectById(projectId);
          if (!found || String(found.ownerId) !== String(chatId)) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          const adminUsername = await reqGetAdminBotUsername();
          const keepAliveId = 'ka_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
          requestAppData.keepAliveThreads[keepAliveId] = {
            projectId:     found.project.id,
            ownerChatId:   chatId,
            ownerName:     `${query.message.chat.first_name || ''} ${query.message.chat.last_name || ''}`.trim() || 'عميل',
            ownerUsername: query.message.chat.username ? '@' + query.message.chat.username : '',
            projectName:   found.project.name,
            projectType:   found.project.type,
            active:        false,
            createdAt:     new Date().toISOString()
          };
          reqSaveLocalData();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: (
              `🔁 *جعل "${found.project.name}" يعمل باستمرار*\n\n` +
              `لتفعيل هذه الخدمة، تواصل مع بوت الإدارة عبر: @${adminUsername || ''}\n\n` +
              `1️⃣ افتح المحادثة مع البوت أعلاه واضغط ابدأ (Start).\n` +
              `2️⃣ انسخ وأرسل له الرسالة التالية بالضبط:\n\n` +
              `\`${keepAliveId}\`\n\n` +
              `سيتعرف بوت الإدارة تلقائياً على مشروعك، وسيتواصل معك المدير قريباً لإخبارك بتكلفة المشروع وتكلفة استمراره.`
            ),
            parse_mode: 'Markdown'
          });
          return;
        }

        // ══════════════════════════════════════════════════════════
        // ═══ تصنيف "إنشاء بوتات تلغرام": خطوات مؤتمتة بأزرار ═══════
        // ══════════════════════════════════════════════════════════
        if (data.startsWith('tgb_')) {
          const session = reqAiSessions[chatId];
          if (!session || session.categoryId !== REQ_CAT_TG_BOT_ID) return;

          // ── نوع عرض الأزرار ──
          if (data === 'tgb_display_inline' || data === 'tgb_display_keyboard') {
            session.tgDisplayMode = data === 'tgb_display_keyboard' ? 'keyboard' : 'inline';
            const label = session.tgDisplayMode === 'keyboard' ? 'Keyboard (لوحة مفاتيح ثابتة)' : 'Inline (أسفل الرسالة)';
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: `✅ تم اختيار: ${label}` });

            session.tgStep = 'files_question';
            const m = reqTgFilesYesNoMsg();
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: m.text, reply_markup: m.reply_markup });
            return;
          }

          // ── سؤال ربط الملفات ──
          if (data === 'tgb_files_yes' || data === 'tgb_files_no') {
            session.tgWantsFiles = (data === 'tgb_files_yes');
            if (!session.tgWantsFiles) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم، لن نربط أي ملفات بأزرار بوتك.' });
              // ننتقل مباشرة لبناء الأزرار عبر الذكاء الاصطناعي
              session.tgStep = 'buttons';
              const sysNote = `[نظام] اختار العميل نوع العرض: ${session.tgDisplayMode === 'keyboard' ? 'Keyboard' : 'Inline'}. اختار العميل عدم ربط أي ملفات بالأزرار. تابع الآن للخطوة 2: اسأله كم زراً يريد وما أسماؤها.`;
              session.aiHistory.push({ role: 'user', content: sysNote });
              const messages = [
                { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
                ...session.aiHistory
              ];
              const aiReply = await reqCallGroqAI(messages);
              if (aiReply) {
                session.aiHistory.push({ role: 'assistant', content: aiReply });
                const clean = reqStripBotDataJson(aiReply.replace('[BUTTONS_DONE]', '').replace('[READY_TO_SEND]', '')).trim();
                await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: clean, parse_mode: 'Markdown' });
              }
              return;
            }
            // نعم: نطلب إنشاء المجموعة، إضافة بوت الطلبات لها كمشرف، وتاغه داخلها
            session.tgStep = 'group_tag_wait';
            session.tgGroupOwnerUserId = chatId; // نفس المستخدم الذي يجب أن يقوم بالتاغ لاحقاً
            const mainUsername = await reqGetMainBotUsername();
            const m = reqTgGroupInstructionsMsg(mainUsername || 'البوت');
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: m.text });
            return;
          }

          // ── تأكيد قيام المستخدم بتاغ بوت الطلبات داخل مجموعته الخاصة ──
          if (data === 'tgb_group_id_confirm') {
            if (session.tgStep !== 'group_tag_wait' || !session.tgPendingGroupId) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: chatId,
                text: '⚠️ لم أستلم بعد أي تاغ لي داخل مجموعتك. تأكد أنك أضفتني للمجموعة كمشرف وقمت بتاغي داخلها، ثم أعد المحاولة.'
              });
              return;
            }
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ التحقق من عضويتك في المجموعة...' });
            // التحقق أن نفس المستخدم (صاحب هذه المحادثة) هو فعلاً عضو في المجموعة التي تاغ منها
            const memberCheck = await fetch(
              `${REQUEST_MAIN_BOT_API}/getChatMember?chat_id=${encodeURIComponent(session.tgPendingGroupId)}&user_id=${encodeURIComponent(chatId)}`
            ).then(r => r.json()).catch(e => ({ ok: false, description: e.message }));

            if (!memberCheck.ok || !['creator', 'administrator', 'member', 'restricted'].includes(memberCheck.result && memberCheck.result.status)) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: chatId,
                text: '❌ تعذّر التحقق من عضويتك في المجموعة. تأكد أنك عضو فيها ثم حاول مجدداً.',
                reply_markup: { inline_keyboard: [[{ text: '✅ تمت المهمة', callback_data: 'tgb_group_id_confirm' }]] }
              });
              return;
            }

            session.tgBotStorageGroupId = session.tgPendingGroupId;
            session.tgStep = 'group_admin_wait';
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم تأكيد المجموعة بنجاح!' });
            const botUsername = (session.pendingBotVerify && session.pendingBotVerify.username) || '';
            const m2 = reqTgGroupAddBotMsg(botUsername);
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: m2.text, reply_markup: m2.reply_markup });
            return;
          }

          // ── تأكيد إضافة البوت كمشرف في المجموعة ──
          if (data === 'tgb_group_added') {
            if (!session.tgBotStorageGroupId || !session.tgBotToken) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ حدث خطأ، لم يتم العثور على بيانات المجموعة أو التوكن. أعد المحاولة من البداية.' });
              return;
            }
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ التحقق من إضافة البوت للمجموعة...' });
            const check = await reqVerifyBotInStorageGroup(session.tgBotToken, session.tgBotStorageGroupId);
            if (!check.ok) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: chatId,
                text: `❌ لم يتم التحقق بنجاح: ${check.error}\n\nتأكد أنك أضفت البوت للمجموعة وجعلته مشرفاً (Admin)، ثم اضغط الزر مجدداً.`,
                reply_markup: { inline_keyboard: [[{ text: '✅ تم، أضفته كمشرف', callback_data: 'tgb_group_added' }]] }
              });
              return;
            }
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم التحقق بنجاح! البوت مشرف في المجموعة.' });
            session.tgStep = 'buttons';
            const sysNote = `[نظام] اختار العميل نوع العرض: ${session.tgDisplayMode === 'keyboard' ? 'Keyboard' : 'Inline'}. اختار العميل ربط ملفات بالأزرار، وتم ربط مجموعة التخزين بنجاح. تابع الآن للخطوة 2: اسأله كم زراً يريد وما أسماؤها.`;
            session.aiHistory.push({ role: 'user', content: sysNote });
            const messages = [
              { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
              ...session.aiHistory
            ];
            const aiReply = await reqCallGroqAI(messages);
            if (aiReply) {
              session.aiHistory.push({ role: 'assistant', content: aiReply });
              const clean = reqStripBotDataJson(aiReply.replace('[BUTTONS_DONE]', '').replace('[READY_TO_SEND]', '')).trim();
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: clean, parse_mode: 'Markdown' });
            }
            return;
          }

          // ── توزيع الأزرار (Layout) ──
          if (data === 'tgb_layout_2col' || data === 'tgb_layout_3col' || data === 'tgb_layout_1col' || data === 'tgb_layout_custom') {
            if (data === 'tgb_layout_custom') {
              session.tgStep = 'layout_custom_wait';
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✍️ اكتب لي كيف تريد أن تظهر الأزرار بالضبط (مثلاً: الزر الأول عرضي كامل، والباقي اثنان بجانب بعض).' });
              return;
            }
            const perRow = data === 'tgb_layout_3col' ? 3 : data === 'tgb_layout_1col' ? 1 : 2;
            const label  = perRow === 3 ? 'ثلاثة أزرار في كل صف' : perRow === 1 ? 'زر واحد عرضي في كل صف' : 'زران بجانب بعض في كل صف';
            session.tgLayoutDescription = label;
            session.tgLayoutPerRow = perRow;
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: `✅ تم اختيار التوزيع: ${label}` });

            session.tgStep = 'add_more';
            const m = reqTgAddMoreMsg();
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: m.text, reply_markup: m.reply_markup });
            return;
          }

          // ── هل تريد إضافة شيء آخر؟ ──
          if (data === 'tgb_addmore_yes') {
            session.tgStep = 'add_more_wait';
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✍️ تفضل، اكتب ما تريد إضافته.' });
            return;
          }
          if (data === 'tgb_addmore_no') {
            session.tgStep = 'summary';
            const sysNote = `[نظام] التوزيع: ${session.tgLayoutDescription || 'تلقائي'}. لم يرغب العميل بإضافة أي شيء آخر. تابع الآن مباشرة لإرسال الملخص النهائي (الخطوة 3).`;
            session.aiHistory.push({ role: 'user', content: sysNote });
            const messages = [
              { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
              ...session.aiHistory
            ];
            const aiReply = await reqCallGroqAI(messages);
            if (aiReply) {
              session.aiHistory.push({ role: 'assistant', content: aiReply });
              const isReady = aiReply.includes('[READY_TO_SEND]');
              const clean = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: clean, parse_mode: 'Markdown' });
              if (isReady) {
                session.done = true;
                session.pendingBotData = reqExtractBotDataJson(aiReply);
                await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                  chat_id: chatId, text: 'إن كان كل شيء صحيحاً اضغط الزر أدناه لتفعيل بوتك فوراً وتلقائياً، أو أخبرني بأي تعديل تريده.',
                  reply_markup: { inline_keyboard: [[{ text: '🚀 ابدأ البناء', callback_data: 'tgb_start_build' }]] }
                });
              }
            }
            return;
          }

          // ── زر بدء البناء ──
          if (data === 'tgb_start_build') {
            if (!session.done || !session.pendingBotData) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ لا يوجد طلب جاهز للبناء حالياً.' });
              return;
            }
            const chatInfoNow = query.message.chat;
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ تفعيل بوتك الآن، لحظات من فضلك...' });

            const buildResult = await reqBuildTelegramBot(session, chatInfoNow);

            if (buildResult.success) {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: chatId,
                text: `🎉 تم تفعيل بوتك بنجاح!\n\n🤖 يمكنك الآن تجربته مباشرة عبر: https://t.me/${buildResult.username}\n\nإذا احتجت أي تعديل لاحقاً، تواصل معنا من جديد.`,
                ...reqBuildMainMenuKeyboard()
              });
              const name  = chatInfoNow.first_name || 'عميل';
              const uname = chatInfoNow.username ? ` @${chatInfoNow.username}` : '';
              await reqNotifyAllAdmins({
                text: (
                  `🤖 *تم بناء بوت تلغرام جديد تلقائياً*\n\n` +
                  `👤 العميل: ${name}${uname}\n` +
                  `🆔 ID: ${chatInfoNow.id}\n` +
                  `🔗 البوت: https://t.me/${buildResult.username}\n` +
                  `🔘 عدد الأزرار: ${buildResult.buttonCount}\n\n` +
                  `ℹ️ هذا للعلم فقط، البوت مُفعَّل ويعمل فعلياً الآن دون الحاجة لأي إجراء إضافي.`
                ),
                parse_mode: 'Markdown'
              });
              const { orderMsgId, photoMsgId, orderMsgIdAdmin, photoMsgIdAdmin } = await reqSendOrderToChannel(session, chatInfoNow).catch(() => ({ orderMsgId: null, photoMsgId: null, orderMsgIdAdmin: null, photoMsgIdAdmin: null }));
              reqSaveOrder(session, chatInfoNow, orderMsgId, photoMsgId, orderMsgIdAdmin, photoMsgIdAdmin);
              // تعليم الطلب كمؤكد تلقائياً (البوت جاهز فوراً دون انتظار المدير) وإضافته لمشاريعي
              const savedOrder = requestAppData.orders[0];
              if (savedOrder) {
                savedOrder.confirmed    = true;
                savedOrder.confirmedAt  = new Date().toISOString();
                savedOrder.confirmedUrl = `https://t.me/${buildResult.username}`;
              }
              reqAddUserProject(chatInfoNow.id, {
                type: 'bot',
                name: `${session.categoryLabel} (@${buildResult.username})`,
                url:  `https://t.me/${buildResult.username}`
              });
              await reqBackupToChannel();
            } else {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: chatId,
                text: `❌ تعذّر تفعيل بوتك: ${buildResult.error}\n\nيرجى التحقق من التوكن أو المجموعة، أو إخباري بالمشكلة لنحاول مرة أخرى.`,
                reply_markup: { inline_keyboard: [[{ text: '🔁 إعادة المحاولة', callback_data: 'tgb_start_build' }]] }
              });
              return;
            }
            delete reqAiSessions[chatId];
            return;
          }

          return;
        }

        return;
      }

      // ─── message ──────────────────────────────────────────────────
      const msg = update.message;
      if (!msg) return;

      // ── رسائل داخل مجموعات: لا نرد إطلاقاً إلا في حالة واحدة فقط ──
      // (مستخدم بانتظار تأكيد مجموعته الخاصة، يقوم بتاغ البوت داخلها)
      if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
        try {
          const mainUsername = await reqGetMainBotUsername();
          const wasMentioned = !!(mainUsername && msg.text && (
            msg.text.includes('@' + mainUsername) ||
            (msg.entities || []).some(e => e.type === 'mention' &&
              msg.text.substring(e.offset, e.offset + e.length) === '@' + mainUsername) ||
            (msg.entities || []).some(e => e.type === 'text_mention' && e.user && mainUsername &&
              String(e.user.username || '').toLowerCase() === mainUsername.toLowerCase())
          ));

          if (wasMentioned && msg.from && !msg.from.is_bot) {
            const ownerId = msg.from.id;
            const ownerSession = reqAiSessions[ownerId];
            if (
              ownerSession &&
              ownerSession.categoryId === REQ_CAT_TG_BOT_ID &&
              ownerSession.tgStep === 'group_tag_wait' &&
              ownerSession.tgGroupOwnerUserId === ownerId
            ) {
              ownerSession.tgPendingGroupId = msg.chat.id;
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: msg.chat.id,
                text: `✅ تمت معرفة id المجموعة للمستخدم: ${ownerId}`,
                reply_to_message_id: msg.message_id
              });
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: ownerId,
                text: `✅ تم استلام معرّف المجموعة من داخل مجموعتك!\n\nارجع الآن واضغط الزر التالي للمتابعة:`,
                reply_markup: { inline_keyboard: [[{ text: '✅ تمت المهمة', callback_data: 'tgb_group_id_confirm' }]] }
              });
            }
          }
        } catch (e) {
          console.error('[ReqBot] خطأ أثناء معالجة رسالة مجموعة:', e.message);
        }
        // بغض النظر عن النتيجة، لا يُسمح لبوت الطلبات بالرد على أي شيء آخر داخل أي مجموعة
        return;
      }

      const chatId   = msg.chat.id;
      const text     = (msg.text || '').trim();
      const chatInfo = msg.chat;

      reqRegisterUser(chatInfo);

      if (reqIsUserBlocked(chatId)) {
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⛔ تم حظرك من استخدام هذا البوت.' });
        return;
      }

      if (text === '/start' || text.startsWith('/start ')) {
        delete reqAiSessions[chatId];
        delete reqUserSessions[chatId];
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: requestAppData.welcome_text, ...reqBuildMainMenuKeyboard() });
        return;
      }

      if (text.startsWith('/')) return;

      // ─── جلسة تعديل بوت تلغرام موجود: محادثة AI تفاعلية + إعادة بناء ──
      if (reqUserSessions[chatId] && reqUserSessions[chatId].action === 'bot_edit_chat') {
        const uSession = reqUserSessions[chatId];
        const botCfg   = reqGetManagedBotConfig(uSession.botId);
        if (!botCfg) {
          delete reqUserSessions[chatId];
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر العثور على البوت، حاول من جديد.' });
          return;
        }

        let userContent = null;

        if (msg.photo?.length > 0 || msg.document) {
          let sourceFileId, mimeGuess, filenameGuess;
          if (msg.photo && msg.photo.length > 0) {
            sourceFileId  = msg.photo[msg.photo.length - 1].file_id;
            mimeGuess     = 'image/jpeg';
            filenameGuess = `photo_${Date.now()}.jpg`;
          } else {
            sourceFileId  = msg.document.file_id;
            mimeGuess     = msg.document.mime_type || 'application/octet-stream';
            filenameGuess = msg.document.file_name || `file_${Date.now()}`;
          }
          const uploadResult = await reqUploadFileToStorage(REQUEST_MAIN_BOT_API, sourceFileId, mimeGuess, filenameGuess);
          const fileIndex = uSession.editFiles.length + 1;
          if (uploadResult.success) {
            uSession.editFiles.push({
              index: fileIndex, fileId: uploadResult.fileId, mime: uploadResult.mime,
              filename: uploadResult.filename, isPhoto: uploadResult.isPhoto, sourceFileId
            });
            userContent = `[أرسل العميل ملفاً جديداً رقم ${fileIndex} (${filenameGuess})]`;
          } else {
            userContent = `[حاول العميل إرسال ملف لكن حدث خطأ تقني في الرفع: ${uploadResult.error}]`;
          }
        } else if (text) {
          userContent = text;
        } else {
          return; // نوع رسالة غير مدعوم بهذه الجلسة
        }

        uSession.editHistory.push({ role: 'user', content: userContent });
        const messages = [
          { role: 'system', content: REQ_TG_BOT_EDIT_SYSTEM(botCfg.buttons || [], botCfg.botName || botCfg.username) },
          ...uSession.editHistory
        ];
        const aiReply = await reqCallGroqAI(messages);
        if (!aiReply) {
          // نزيل رسالة العميل الأخيرة من السجل حتى لا تتكرر مرتين عند إعادة المحاولة
          uSession.editHistory.pop();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: '😔 حدث خطأ تقني مؤقت في الاتصال بالذكاء الاصطناعي. أعد إرسال رسالتك من فضلك، أو حاول بعد قليل.',
            reply_markup: { inline_keyboard: [[{ text: '🔁 إعادة المحاولة', callback_data: `projedit_retry_${uSession.projectId}` }]] }
          });
          return;
        }
        uSession.editHistory.push({ role: 'assistant', content: aiReply });
        const isReady   = aiReply.includes('[READY_TO_SEND]');
        const cleanReply = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanReply, parse_mode: 'Markdown' });

        if (isReady) {
          const updatedData = reqExtractBotDataJson(aiReply);
          if (updatedData && Array.isArray(updatedData.buttons)) {
            uSession.pendingEditButtons = updatedData.buttons;
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: '🚀 التعديلات جاهزة، اضغط الزر أدناه لإعادة بناء البوت بها الآن:',
              reply_markup: { inline_keyboard: [[{ text: '🔁 إعادة البناء', callback_data: 'bot_edit_rebuild' }]] }
            });
          } else {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ حدث خطأ تقني في تجهيز التعديلات، حاول وصف طلبك مجدداً.' });
          }
        }
        return;
      }

      // ─── جلسة تعديل مشروع: استلام وصف التعديل ومعالجته بالذكاء الاصطناعي ──
      if (reqUserSessions[chatId] && reqUserSessions[chatId].action === 'project_edit_describe' && text) {
        const uSession  = reqUserSessions[chatId];
        const found     = reqFindProjectById(uSession.projectId);
        delete reqUserSessions[chatId];

        if (!found || String(found.ownerId) !== String(chatId)) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر العثور على المشروع، حاول من جديد عبر قسم "مشاريعي".' });
          return;
        }
        const project = found.project;

        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ صياغة طلب التعديل...' });

        // صياغة ملخص احترافي دقيق للتعديل المطلوب عبر الذكاء الاصطناعي
        const editSystemPrompt =
          'أنت مساعد يصيغ طلبات تعديل تقنية دقيقة وواضحة لمطوّر ويب، بالعربية فقط. ' +
          'المستخدم وصف تعديلاً يريده على موقعه/تطبيقه/بوته. أعد صياغة وصفه في ملخص منظم وواضح ودقيق بالنقاط، ' +
          'بدون أي مقدمات أو خاتمة، فقط النقاط التقنية المطلوب تنفيذها. إن كان الوصف غامضاً حافظ على المعنى دون اختراع تفاصيل غير مذكورة.';
        const aiSummary = await reqCallGroqAI([
          { role: 'system', content: editSystemPrompt },
          { role: 'user', content: text }
        ]).catch(() => null);
        const finalSummary = (aiSummary && aiSummary.trim()) ? aiSummary.trim() : text;

        const ownerUname = chatInfo.username ? `@${chatInfo.username}` : '(لا يوجد يوزر)';
        const ownerName   = `${chatInfo.first_name || ''} ${chatInfo.last_name || ''}`.trim() || 'عميل';

        const adminText = (
          `✏️ *طلب تعديل مشروع*\n\n` +
          `👤 العميل: ${ownerName} ${ownerUname}\n` +
          `🆔 ID: \`${chatId}\`\n` +
          `🏷 المشروع: ${project.name} (${reqProjectTypeLabel(project.type)})\n` +
          (project.url ? `🔗 ${project.url}\n` : '') +
          `\n📝 *ملخص التعديل المطلوب:*\n${finalSummary}`
        );

        await reqNotifyAllAdmins({ text: adminText, parse_mode: 'Markdown' });

        // إرسال ملف index.html المخزّن (إن وُجد) لكل مدير
        if (project.indexMsgId) {
          for (const adminId of REQ_ADMIN_IDS) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'forwardMessage', {
              chat_id: adminId,
              from_chat_id: REQUEST_BACKUP_CHANNEL,
              message_id: project.indexMsgId
            }).catch(() => {});
          }
        }

        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: '✅ تم إرسال طلب التعديل للإدارة، سيتم التواصل معك قريباً.'
        });
        return;
      }

      // ─── زر الطلب السابق ─────────────────────────────────────────
      if (text === REQ_BTN_PREV_ORDER) {
        const orders = (requestAppData.orders || []).filter(o => String(o.chatId) === String(chatId));
        if (!orders.length) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ لم تقم بتقديم أي طلب سابق.' });
          return;
        }
        const lastOrder = orders[0];
        if (lastOrder.confirmed) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `✅ طلبك السابق تم تأكيده!\n\n🏷 النوع: ${lastOrder.categoryLabel}\n📅 ${lastOrder.dateStr}  🕐 ${lastOrder.timeStr}\n\nيمكنك الاطلاع على نتيجته في قسم 🌐 مواقعي`
          });
        } else {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: (
              `🏷 *${lastOrder.categoryLabel}*\n` +
              `👤 ${lastOrder.clientName}\n` +
              `📅 ${lastOrder.dateStr}  🕐 ${lastOrder.timeStr}\n\n` +
              `⏳ نعتذر منك، لم يتم إنشاء طلبك بعد بسبب الضغط الكبير على الفريق.\n` +
              `يرجى الانتظار وسيتم التواصل معك في أقرب وقت ممكن. 🙏`
            ),
            parse_mode: 'Markdown'
          });
        }
        return;
      }

      // ─── زر مشاريعي ─────────────────────────────────────────────
      if (text === REQ_BTN_MY_SITES) {
        const projects = reqGetUserProjects(chatId);
        if (!projects.length) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: 'ℹ️ لا توجد مشاريع مؤكدة لك بعد.' });
          return;
        }
        const btns = projects.map(p => [{
          text: `${p.active === false ? '🔴' : '🟢'} ${reqProjectTypeLabel(p.type)} — ${p.name}`,
          callback_data: `myproj_${p.id}`
        }]);
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: '📦 *مشاريعي:*\n\nاختر مشروعاً لعرض تفاصيله وإدارته:',
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: btns }
        });
        return;
      }

      // ─── زر تحويل مشروعي إلى دائم ─────────────────────────────
      if (text === REQ_BTN_MAKE_PERMANENT) {
        reqPermanentSessions[chatId] = { step: 'choose_type' };
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: '🚀 *تحويل مشروعي إلى دائم*\n\nاختر نوع المشروع الذي تريد تحويله إلى دائم:',
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: [
            [{ text: '📱 تطبيقاتي (APK)',      callback_data: 'permtype_apk' }],
            [{ text: '🌐 مواقعي الإلكترونية', callback_data: 'permtype_website' }],
            [{ text: '🤖 بوتاتي التلغرام',    callback_data: 'permtype_bot' }]
          ] }
        });
        return;
      }

      // ─── إدخال بيانات التحويل (TxID) بعد الضغط على "لقد قمت بالتحويل" ──
      if (reqPermanentSessions[chatId] && reqPermanentSessions[chatId].step === 'await_txid') {
        const pSession = reqPermanentSessions[chatId];
        const txId = text.trim();
        if (!txId) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ يرجى إدخال رقم/رمز عملية التحويل (Transaction ID / Hash) بشكل صحيح.' });
          return;
        }
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ التحقق من عملية التحويل، الرجاء الانتظار...' });

        const project = pSession.project;
        const result = await reqBinanceCheckDeposit({
          txId,
          amount: project ? project.permanentPrice : null,
          address: REQ_PERMANENT_PAY_ADDRESS
        });

        if (result.ok) {
          if (project) {
            project.permanentPaymentTxId  = txId;
            project.permanentPaymentAt    = new Date().toISOString();
            project.permanentPaymentState = 'confirmed';
            reqSaveLocalData();
          }
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: (
              `✅ تم تأكيد استلام التحويل بنجاح!\n\n` +
              `سيقوم المدير بجعل ${project ? reqProjectTypeLabel(project.type) : 'مشروعك'} دائماً ويعمل بكفاءة.\n` +
              `سيتم إرسال ${project && project.type === 'apk' ? 'تطبيقك' : (project && project.type === 'bot' ? 'بوتك' : 'موقعك')} الدائم في أقرب وقت. 🙏`
            )
          });
          // إشعار المدير بضرورة تفعيل المشروع كدائم
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: REQUEST_ADMIN_ID,
            text: (
              `💰 *تم تأكيد دفعة "تحويل إلى دائم"*\n\n` +
              `👤 المستخدم: ${chatId}\n` +
              `📦 المشروع: ${project ? project.name : '—'}\n` +
              `🏷 النوع: ${project ? reqProjectTypeLabel(project.type) : '—'}\n` +
              `🔗 TxID: \`${txId}\`\n\n` +
              `يرجى جعل المشروع دائماً الآن.`
            ),
            parse_mode: 'Markdown'
          });
        } else if (result.error === 'missing_api_keys') {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: '⚠️ تعذّر التحقق التلقائي حالياً، سيتم مراجعة عملية التحويل يدوياً من قبل المدير وسنعلمك فور التأكيد.'
          });
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: REQUEST_ADMIN_ID,
            text: `⚠️ مستخدم ${chatId} أدخل TxID: \`${txId}\` لمشروع "${project ? project.name : '—'}" لكن مفاتيح Binance API غير مُعرّفة على السيرفر.`,
            parse_mode: 'Markdown'
          });
        } else {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: '❌ لم يتم العثور على عملية تحويل مطابقة لهذه البيانات. تأكد من صحة رمز العملية (TxID) وحاول مرة أخرى، أو تواصل مع الدعم.',
            reply_markup: { inline_keyboard: [[{ text: '🔁 إعادة إدخال رمز التحويل', callback_data: 'permpay_retry_txid' }]] }
          });
        }
        delete reqPermanentSessions[chatId];
        return;
      }

      // ─── اختيار تصنيف عبر نص الزر (Reply Keyboard) ───────────────
      // ⚠️ يُسمح باختيار تصنيف جديد في أي وقت، حتى لو كانت هناك جلسة أخرى قيد العمل
      // (بما فيها انتظار رد الذكاء الاصطناعي)؛ في هذه الحالة تُوقَف الجلسة الحالية فوراً
      // وتُستبدل بالتصنيف المكبوس حديثاً. نستخدم عداد أجيال (aiGen) لكل جلسة حتى لا يُكتب
      // أي رد ذكاء اصطناعي "متأخر" يعود لجلسة/تصنيف سابق تم التخلي عنه.
      let session = reqAiSessions[chatId];
      {
        const category = (requestAppData.categories || []).find(c => c.label === text);
        if (category) {
          // التحقق من وجود طلب سابق غير مؤكد
          const userOrders = (requestAppData.orders || []).filter(o => String(o.chatId) === String(chatId));
          const lastOrder  = userOrders[0];
          if (lastOrder && !lastOrder.confirmed) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: (
                `⚠️ لديك طلب سابق قيد المراجعة:\n\n` +
                `🏷 *${lastOrder.categoryLabel}*\n` +
                `📅 ${lastOrder.dateStr}  🕐 ${lastOrder.timeStr}\n\n` +
                `لا يمكنك تقديم طلب جديد حتى يُراجع المدير طلبك الحالي ويؤكده.\n` +
                `يرجى الانتظار. 🙏`
              ),
              parse_mode: 'Markdown'
            });
            return;
          }

          // إن كانت هناك جلسة سابقة قيد العمل (لم تُنجز بعد)، أوقفها فوراً عبر رفع رقم الجيل
          // بحيث أي رد ذكاء اصطناعي قادم من تلك الجلسة القديمة يُتجاهل عند وصوله لاحقاً
          const previousGen = (reqAiSessions[chatId] && reqAiSessions[chatId].aiGen) || 0;
          const newGen = previousGen + 1;

          const clientName = `${chatInfo.first_name || ''} ${chatInfo.last_name || ''}`.trim() || 'عميل';

          reqAiSessions[chatId] = {
            categoryId:    category.id,
            categoryLabel: category.label,
            categoryKind:  category.kind === 'app' ? 'app' : 'website', // 'website' (موقع) أو 'app' (تطبيق Kotlin) — الافتراضي موقع للحفاظ على التوافق مع التصنيفات القديمة
            clientName,
            aiHistory:     [],
            photoFileId:   null,
            done:          false,
            tgStep:        category.id === REQ_CAT_TG_BOT_ID ? 'token' : null,
            aiGen:         newGen
          };

          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `✅ اخترت: *${category.label}*\n\nسأساعدك في تقديم طلبك بشكل مفصّل. سأطرح عليك بعض الأسئلة خطوة بخطوة 📝`,
            parse_mode: 'Markdown'
          });

          const systemMsg  = { role: 'system', content: REQ_AI_SYSTEM(category.label, clientName) };
          const firstInput = { role: 'user', content: `بدأت المحادثة. العميل اختار تصنيف: ${category.label}` };
          const aiReply    = await reqCallGroqAI([systemMsg, firstInput]);

          // إن تغيّر الجيل أثناء الانتظار (المستخدم بدّل التصنيف مرة أخرى)، تجاهل هذا الرد نهائياً
          const stillCurrent = reqAiSessions[chatId] && reqAiSessions[chatId].aiGen === newGen;
          if (aiReply && stillCurrent) {
            reqAiSessions[chatId].aiHistory.push(firstInput);
            reqAiSessions[chatId].aiHistory.push({ role: 'assistant', content: aiReply });
            const cleanReply = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
              chat_id: chatId, text: cleanReply, parse_mode: 'Markdown'
            });
          }
          return;
        }
        if (!session || session.done) return;
      }

      // ── استلام ملف (صورة/مستند) خاص بتصنيف "إنشاء بوتات تلغرام" فقط ──
      // يُرفع فوراً لقناة التخزين الدائمة، ويُسجَّل بترتيبه ضمن الجلسة
      // ليستخدمه الذكاء الاصطناعي لاحقاً في كتلة BOT_DATA_JSON (fileRef)
      if (session.categoryId === REQ_CAT_TG_BOT_ID && (msg.photo?.length > 0 || msg.document)) {
        if (!session.tgBotFiles) session.tgBotFiles = [];

        let sourceFileId, mimeGuess, filenameGuess;
        if (msg.photo && msg.photo.length > 0) {
          sourceFileId  = msg.photo[msg.photo.length - 1].file_id;
          mimeGuess     = 'image/jpeg';
          filenameGuess = `photo_${Date.now()}.jpg`;
        } else {
          sourceFileId  = msg.document.file_id;
          mimeGuess     = msg.document.mime_type || 'application/octet-stream';
          filenameGuess = msg.document.file_name || `file_${Date.now()}`;
        }

        const uploadResult = await reqUploadFileToStorage(REQUEST_MAIN_BOT_API, sourceFileId, mimeGuess, filenameGuess);
        const fileIndex = session.tgBotFiles.length + 1; // ترقيم يبدأ من 1 كما يتوقعه الـ AI في fileRef

        let userContent;
        if (uploadResult.success) {
          session.tgBotFiles.push({
            index: fileIndex, fileId: uploadResult.fileId, mime: uploadResult.mime,
            filename: uploadResult.filename, isPhoto: uploadResult.isPhoto, sourceFileId
          });
          userContent = `[أرسل العميل ملفاً رقم ${fileIndex} (${filenameGuess})]`;
        } else {
          userContent = `[حاول العميل إرسال ملف لكن حدث خطأ تقني في الرفع: ${uploadResult.error}]`;
        }

        session.aiHistory.push({ role: 'user', content: userContent });
        const messages = [
          { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
          ...session.aiHistory
        ];
        const aiReply = await reqCallGroqAI(messages);
        if (!aiReply) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: uploadResult.success ? '✅ تم استلام الملف!' : '⚠️ حدث خطأ في استلام الملف، حاول إرساله مرة أخرى.' });
          return;
        }
        session.aiHistory.push({ role: 'assistant', content: aiReply });
        const isReadyF    = aiReply.includes('[READY_TO_SEND]');
        const cleanReplyF = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanReplyF, parse_mode: 'Markdown' });
        if (isReadyF) {
          session.done = true;
          session.pendingBotData = reqExtractBotDataJson(aiReply);
        }
        return;
      }

      // ── استلام صورة/أيقونة ───────────────────────────────────────
      if (msg.photo && msg.photo.length > 0) {
        session.photoFileId = msg.photo[msg.photo.length - 1].file_id;
        const userContent = '[أرسل العميل صورة/أيقونة]';
        session.aiHistory.push({ role: 'user', content: userContent });
        const messages = [
          { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
          ...session.aiHistory
        ];
        const aiReply = await reqCallGroqAI(messages);
        if (!aiReply) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم استلام الصورة!' });
          session.aiHistory.push({ role: 'assistant', content: '✅ تم استلام الصورة!' });
          return;
        }
        session.aiHistory.push({ role: 'assistant', content: aiReply });
        const isReady    = aiReply.includes('[READY_TO_SEND]');
        const cleanReply = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanReply, parse_mode: 'Markdown' });
        if (isReady) {
          session.done = true;
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text: '📤 هل تود إرسال هذا الطلب للمدير؟',
            reply_markup: { inline_keyboard: [
              [{ text: '✅ إرسال الطلب', callback_data: 'confirm_request' }],
              [{ text: '❌ إلغاء',        callback_data: 'cancel_request'  }]
            ]}
          });
        }
        return;
      }

      // ── استلام رسالة صوتية ───────────────────────────────────────
      if (msg.voice) {
        try {
          const fileInfo = await reqSendToTg(REQUEST_MAIN_BOT_API, 'getFile', { file_id: msg.voice.file_id });
          if (!fileInfo.ok || !fileInfo.result.file_path) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر معالجة الرسالة الصوتية.' });
            return;
          }
          const voiceUrl    = `https://api.telegram.org/file/bot${REQUEST_MAIN_BOT_TOKEN}/${fileInfo.result.file_path}`;
          const voiceRes    = await fetch(voiceUrl);
          if (!voiceRes.ok) throw new Error('فشل تنزيل الصوت');
          const voiceBuffer = Buffer.from(await voiceRes.arrayBuffer());
          const voiceBase64 = voiceBuffer.toString('base64');
          const gemRes = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-goog-api-key': GEMINI_API_KEY },
            body: JSON.stringify({ contents: [{ parts: [
              { inline_data: { mime_type: 'audio/ogg', data: voiceBase64 } },
              { text: 'حوّل هذا الصوت إلى نص عربي فقط بدون أي تعليق.' }
            ]}]})
          });
          const gemData  = await gemRes.json();
          const transcript = gemData.candidates?.[0]?.content?.parts?.[0]?.text?.trim() || '';
          if (!transcript) {
            await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '🎤 لم أتمكن من فهم الصوت، الرجاء الكتابة.' });
            return;
          }
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: `🎤 فهمت: "${transcript}"` });
          session.aiHistory.push({ role: 'user', content: transcript });
          const messages = [
            { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
            ...session.aiHistory
          ];
          const aiReply = await reqCallGroqAI(messages);
          if (!aiReply) { await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '😔 خطأ مؤقت.' }); return; }
          session.aiHistory.push({ role: 'assistant', content: aiReply });
          const isReadyV    = aiReply.includes('[READY_TO_SEND]');
          const cleanReplyV = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanReplyV, parse_mode: 'Markdown' });
          if (isReadyV) {
            session.done = true;
            if (session.categoryId === REQ_CAT_TG_BOT_ID) {
              session.pendingBotData = reqExtractBotDataJson(aiReply);
            } else {
              await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
                chat_id: chatId, text: '📤 هل تود إرسال هذا الطلب للمدير؟',
                reply_markup: { inline_keyboard: [
                  [{ text: '✅ إرسال الطلب', callback_data: 'confirm_request' }],
                  [{ text: '❌ إلغاء',        callback_data: 'cancel_request'  }]
                ]}
              });
            }
          }
        } catch(voiceErr) {
          console.error('[ReqBot Voice] خطأ:', voiceErr.message);
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ خطأ في الصوت، حاول الكتابة.' });
        }
        return;
      }

      // ── تصنيف بوتات تلغرام: تذكير أثناء انتظار تاغ البوت داخل المجموعة الخاصة ──
      if (session.categoryId === REQ_CAT_TG_BOT_ID && session.tgStep === 'group_tag_wait' && msg.text) {
        const mainUsername = await reqGetMainBotUsername();
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: (
            `📦 بانتظار تاغك لي داخل مجموعتك الخاصة.\n\n` +
            `تأكد من:\n` +
            `1️⃣ إنشاء المجموعة.\n` +
            `2️⃣ إضافتي إليها (@${mainUsername || ''}) وجعلي مشرفاً.\n` +
            `3️⃣ كتابة رسالة تتضمن تاغي (@${mainUsername || ''}) داخل المجموعة.`
          )
        });
        return;
      }

      // ── تصنيف بوتات تلغرام: استلام وصف توزيع مخصص كتابةً ──
      if (session.categoryId === REQ_CAT_TG_BOT_ID && session.tgStep === 'layout_custom_wait' && text) {
        session.tgLayoutDescription = text;
        session.tgLayoutPerRow = null; // توزيع مخصص، سيحدده الذكاء الاصطناعي عبر row/rowWidth بحرية
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: `✅ تم تسجيل التوزيع المخصص: ${text}` });
        session.tgStep = 'add_more';
        const m = reqTgAddMoreMsg();
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: m.text, reply_markup: m.reply_markup });
        return;
      }

      // ── تصنيف بوتات تلغرام: استلام "إضافة أخرى" كتابةً، ثم الانتقال للملخص ──
      if (session.categoryId === REQ_CAT_TG_BOT_ID && session.tgStep === 'add_more_wait' && text) {
        session.tgStep = 'summary';
        const sysNote = `[نظام] التوزيع: ${session.tgLayoutDescription || 'تلقائي'}. أضاف العميل الملاحظة التالية: "${text}". تابع الآن مباشرة لإرسال الملخص النهائي (الخطوة 3) مع تضمين هذه الملاحظة.`;
        session.aiHistory.push({ role: 'user', content: sysNote });
        const messages = [
          { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
          ...session.aiHistory
        ];
        const aiReply = await reqCallGroqAI(messages);
        if (!aiReply) {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '😔 حدث خطأ مؤقت، الرجاء إعادة المحاولة.' });
          return;
        }
        session.aiHistory.push({ role: 'assistant', content: aiReply });
        const isReady = aiReply.includes('[READY_TO_SEND]');
        const clean = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: clean, parse_mode: 'Markdown' });
        if (isReady) {
          session.done = true;
          session.pendingBotData = reqExtractBotDataJson(aiReply);
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text: 'إن كان كل شيء صحيحاً اضغط الزر أدناه لتفعيل بوتك فوراً وتلقائياً، أو أخبرني بأي تعديل تريده.',
            reply_markup: { inline_keyboard: [[{ text: '🚀 ابدأ البناء', callback_data: 'tgb_start_build' }]] }
          });
        }
        return;
      }

      // ── رسالة نصية عادية → AI ────────────────────────────────────
      if (!text) return;

      session.aiHistory.push({ role: 'user', content: text });
      const messages = [
        { role: 'system', content: REQ_AI_SYSTEM(session.categoryLabel, session.clientName) },
        ...session.aiHistory
      ];
      const aiReply = await reqCallGroqAI(messages);
      if (!aiReply) {
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '😔 حدث خطأ مؤقت، الرجاء إعادة المحاولة.' });
        return;
      }
      session.aiHistory.push({ role: 'assistant', content: aiReply });

      // ── تصنيف بوتات تلغرام: اعتراض علامات الانتقال المؤتمت ──
      if (session.categoryId === REQ_CAT_TG_BOT_ID) {
        if (aiReply.includes('[TOKEN_RECEIVED]')) {
          const cleanTok = aiReply.replace('[TOKEN_RECEIVED]', '').trim();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanTok, parse_mode: 'Markdown' });
          // استخراج التوكن من نص المحادثة (آخر رسالة user تحتوي توكناً بالشكل الصحيح)
          const tokenMatch = [...session.aiHistory].reverse().find(m => m.role === 'user' && /\d+:[A-Za-z0-9_-]{30,}/.test(m.content));
          const tokenStr = tokenMatch ? tokenMatch.content.match(/\d+:[A-Za-z0-9_-]{30,}/)[0] : null;
          session.tgBotToken = tokenStr;
          if (tokenStr) {
            const verifyNow = await reqVerifyBotToken(tokenStr);
            if (verifyNow.valid) session.pendingBotVerify = verifyNow;
          }
          session.tgStep = 'display_mode';
          const m = reqTgDisplayModeMsg();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: m.text, reply_markup: m.reply_markup });
          return;
        }
        if (aiReply.includes('[BUTTONS_DONE]')) {
          const cleanBd = aiReply.replace('[BUTTONS_DONE]', '').trim();
          if (cleanBd) await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanBd, parse_mode: 'Markdown' });
          session.tgStep = 'layout';
          const m = reqTgLayoutMsg();
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: m.text, reply_markup: m.reply_markup });
          return;
        }
      }

      const isReady    = aiReply.includes('[READY_TO_SEND]');
      const cleanReply = reqStripBotDataJson(aiReply.replace('[READY_TO_SEND]', '')).trim();
      await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', { chat_id: chatId, text: cleanReply, parse_mode: 'Markdown' });
      if (isReady) {
        session.done = true;
        if (session.categoryId === REQ_CAT_TG_BOT_ID) {
          // ── تصنيف بوتات تلغرام: لا ننتظر المدير، فقط زر تفاعلي لبدء البناء ──
          session.pendingBotData = reqExtractBotDataJson(aiReply);
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text: 'إن كان كل شيء صحيحاً اضغط الزر أدناه لتفعيل بوتك فوراً وتلقائياً، أو أخبرني بأي تعديل تريده.',
            reply_markup: { inline_keyboard: [[{ text: '🚀 ابدأ البناء', callback_data: 'tgb_start_build' }]] }
          });
        } else {
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text: '📤 هل تود إرسال هذا الطلب للمدير؟',
            reply_markup: { inline_keyboard: [
              [{ text: '✅ إرسال الطلب', callback_data: 'confirm_request' }],
              [{ text: '❌ إلغاء',        callback_data: 'cancel_request'  }]
            ]}
          });
        }
      }

    } catch(e) { console.error('[RequestBot Main] خطأ:', e.message); }
  });
});


// ══════════════════════════════════════════════════════════════════════
// ═══ بوت الطلبات: webhook البوت الإداري ══════════════════════════════
// ══════════════════════════════════════════════════════════════════════
app.post('/request-bot-webhook/admin', (req, res) => {
  res.sendStatus(200);
  setImmediate(async () => {
    try {
      const update = req.body;
      if (!update) return;

      // ─── callback_query (للأزرار الديناميكية التي تحمل معرّفات) ────
      if (update.callback_query) {
        const query  = update.callback_query;
        const msg    = query.message;
        const chatId = msg.chat.id;
        const data   = query.data;

        if (!reqIsAdmin(chatId)) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'answerCallbackQuery', { callback_query_id: query.id, text: '⛔ غير مصرح لك.' });
          return;
        }

        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'answerCallbackQuery', { callback_query_id: query.id });

        // ── عرض تفاصيل طلب معيّن ──────────────────────────────────
        if (data.startsWith('vieworder_')) {
          const orderId = parseInt(data.replace('vieworder_', ''));
          const order   = (requestAppData.orders || []).find(o => o.id === orderId);
          if (!order) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ الطلب غير موجود.', ...reqBuildSitesMenuKeyboard() });
            return;
          }

          const confirmedBadge = order.confirmed ? '✅ مؤكد' : '⏳ قيد الانتظار';
          const summary =
            `📋 *تفاصيل الطلب*\n\n` +
            `🏷 النوع: ${order.categoryLabel}\n` +
            `📅 ${order.dateStr}  🕐 ${order.timeStr}\n` +
            `👤 ${order.clientName} ${order.clientUsername}\n` +
            `🆔 Chat ID: \`${order.chatId}\`\n` +
            `💬 عدد الردود: ${order.historyCount}\n` +
            `📎 صورة: ${order.hasPhoto ? '✅' : '❌'}\n` +
            `📌 الحالة: ${confirmedBadge}\n\n` +
            `📄 الطلب الكامل بالأسفل ⬇️`;

          const orderBtns = [[{ text: '🔙 عودة لقائمة الطلبات', callback_data: 'admin_list_orders' }]];
          if (!order.confirmed) {
            orderBtns.unshift([{ text: '✅ تأكيد الطلب وإرسال الرابط', callback_data: `confirm_order_${order.id}` }]);
          }

          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id:      chatId,
            text:         summary,
            parse_mode:   'Markdown',
            reply_markup: { inline_keyboard: orderBtns }
          });

          if (order.channelMsgIdAdmin || order.channelMsgId) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'forwardMessage', {
              chat_id:      chatId,
              from_chat_id: REQUEST_BACKUP_CHANNEL,
              message_id:   order.channelMsgIdAdmin || order.channelMsgId
            }).catch(async (e) => {
              console.error('[ReqBot] فشل forward الطلب:', e.message);
              await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
                chat_id: chatId,
                text: '⚠️ تعذّر جلب الملف من المجموعة. تأكد أن البوت الإداري مضاف في المجموعة ومشرف فيها.'
              });
            });
          } else {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
              chat_id: chatId,
              text: '⚠️ لا يوجد رابط لرسالة المجموعة لهذا الطلب (قد يكون قديماً).'
            });
          }

          if (order.photoMsgIdAdmin || order.photoMsgId) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'forwardMessage', {
              chat_id:      chatId,
              from_chat_id: REQUEST_BACKUP_CHANNEL,
              message_id:   order.photoMsgIdAdmin || order.photoMsgId
            }).catch(() => {});
          }
          return;
        }

        // ── تأكيد الطلب من المدير: طلب رابط الموقع ────────────────
        if (data.startsWith('confirm_order_')) {
          const orderId = parseInt(data.replace('confirm_order_', ''));
          const order   = (requestAppData.orders || []).find(o => o.id === orderId);
          if (!order) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ الطلب غير موجود.' });
            return;
          }
          if (order.confirmed) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ هذا الطلب مؤكد مسبقاً.' });
            return;
          }
          reqAdminSessions[chatId] = { action: 'confirm_order_url', orderId };
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `🔗 أدخل رابط الموقع المُنجز للعميل *${order.clientName}*:\n\n(مثال: https://example.com)`,
            parse_mode: 'Markdown'
          });
          return;
        }

        // ── إعادة عرض قائمة الطلبات (زر الرجوع من تفاصيل الطلب) ───
        if (data === 'admin_list_orders') {
          const orders = requestAppData.orders || [];
          if (!orders.length) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا يوجد طلبات مسجلة بعد.', ...reqBuildSitesMenuKeyboard() });
            return;
          }
          const btns = orders.slice(0, 15).map((o, i) => [{
            text: `${i + 1}. ${o.categoryLabel} — ${o.clientName} (${o.dateStr})`,
            callback_data: `vieworder_${o.id}`
          }]);
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id:      chatId,
            text:         `📨 *قائمة الطلبات* — آخر ${Math.min(orders.length, 15)} طلب (المجموع: ${orders.length})`,
            parse_mode:   'Markdown',
            reply_markup: { inline_keyboard: btns }
          });
          return;
        }

        // ── تفعيل/إيقاف موقع مؤقت (من قائمة "المواقع") ────────────
        if (data.startsWith('admsite_toggle_')) {
          const slug = data.replace('admsite_toggle_', '');
          const site = tempHostSites.get(slug);
          if (!site) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ الموقع غير موجود (قد يكون منتهياً).' });
            return;
          }
          site.active = !site.active;
          thUpdateFirestore(slug, { active: site.active }).catch(() => {});
          const status = site.active ? '🟢 يعمل' : '🔴 متوقف';
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `✅ تم تحديث حالة الموقع.\n\n🏷 ${site.label || slug}\n📊 الحالة الآن: ${status}`,
            reply_markup: { inline_keyboard: [
              [{ text: site.active ? '🔴 إيقاف' : '🟢 تشغيل', callback_data: `admsite_toggle_${slug}` }],
              [{ text: '🔙 كل المواقع', callback_data: 'admin_list_sites' }]
            ]}
          });
          return;
        }

        // ── عرض كل المواقع المؤقتة (نشطة/متوقفة) ──────────────────
        if (data === 'admin_list_sites') {
          if (!tempHostSites.size) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا يوجد مواقع مؤقتة مسجلة حالياً.', ...reqBuildSitesMenuKeyboard() });
            return;
          }
          const now  = Date.now();
          const btns = [];
          let text   = '🌐 *المواقع المؤقتة المسجّلة:*\n\n';
          let i = 1;
          for (const [slug, site] of tempHostSites.entries()) {
            const remain = Math.max(0, Math.floor((site.expiresAt - now) / 3600000));
            text += `${i}. ${site.active ? '🟢' : '🔴'} ${site.label || slug} — يبقى ${remain} ساعة | 👤 ${site.ownerId}\n`;
            btns.push([{ text: `${site.active ? '🟢' : '🔴'} ${site.label || slug}`, callback_data: `admsite_toggle_${slug}` }]);
            i++;
          }
          text += '\nاضغط على أي موقع لتفعيله أو إيقافه.';
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text, parse_mode: 'Markdown', reply_markup: { inline_keyboard: btns }
          });
          return;
        }

        // ── عرض تفاصيل مستخدم محدد (مراسلة / مواقعه / حظر) ────────
        if (data.startsWith('admuser_')) {
          const targetId = data.replace('admuser_', '');
          const u = reqGetUser(targetId);
          if (!u) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المستخدم غير موجود.' });
            return;
          }
          const userSites = thBuildSiteList(targetId);
          const text =
            `👤 *${reqUserDisplayName(u)}*\n\n` +
            `🆔 \`${u.chatId}\`\n` +
            `📅 أول ظهور: ${new Date(u.firstSeen).toLocaleDateString('ar-SY')}\n` +
            `🕐 آخر نشاط: ${new Date(u.lastSeen).toLocaleDateString('ar-SY')}\n` +
            `🌐 عدد المواقع المنشأة: ${userSites.length}\n` +
            `🚦 الحالة: ${u.blocked ? '⛔ محظور' : '✅ غير محظور'}`;
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text, parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [
              [{ text: '✉️ مراسلة المستخدم',          callback_data: `admmsguser_${u.chatId}` }],
              [{ text: `🌐 المواقع المنشأة (${userSites.length})`, callback_data: `admusersites_${u.chatId}` }],
              [{ text: u.blocked ? '✅ رفع الحظر' : '⛔ حظر المستخدم', callback_data: `admblock_${u.chatId}` }],
              [{ text: '🔙 كل المستخدمين', callback_data: 'admin_list_users' }]
            ]}
          });
          return;
        }

        // ── إعادة عرض قائمة كل المستخدمين ─────────────────────────
        if (data === 'admin_list_users') {
          const users = Object.values(requestAppData.users || {});
          if (!users.length) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا يوجد مستخدمون مسجلون بعد.', ...reqBuildAdminMenuKeyboard() });
            return;
          }
          const btns = users.map(u => [{ text: `${u.blocked ? '⛔' : '👤'} ${reqUserDisplayName(u)}`, callback_data: `admuser_${u.chatId}` }]);
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `👥 *كل المستخدمين* (${users.length}):\n\nاضغط على اسم المستخدم لعرض خياراته.`,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: btns }
          });
          return;
        }

        // ── بدء مراسلة مستخدم محدد ─────────────────────────────────
        if (data.startsWith('admmsguser_')) {
          const targetId = data.replace('admmsguser_', '');
          reqAdminSessions[chatId] = { action: 'msg_user', targetId };
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✏️ اكتب الرسالة التي تريد إرسالها لهذا المستخدم:\n\n(للإلغاء اكتب /cancel)' });
          return;
        }

        // ── عرض مواقع مستخدم محدد مع إمكانية تفعيل/إيقاف ──────────
        if (data.startsWith('admusersites_')) {
          const targetId = data.replace('admusersites_', '');
          const sites = thBuildSiteList(targetId);
          if (!sites.length) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
              chat_id: chatId, text: '📭 هذا المستخدم لا يملك مواقع منشأة.',
              reply_markup: { inline_keyboard: [[{ text: '🔙 رجوع', callback_data: `admuser_${targetId}` }]] }
            });
            return;
          }
          const now = Date.now();
          const btns = sites.map(s => {
            const remain = Math.max(0, Math.floor((s.expiresAt - now) / 3600000));
            return [{ text: `${s.active ? '🟢' : '🔴'} ${s.label || s.slug} (${remain}س)`, callback_data: `admsite_toggle_${s.slug}` }];
          });
          btns.push([{ text: '🔙 رجوع لملف المستخدم', callback_data: `admuser_${targetId}` }]);
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `🌐 *مواقع المستخدم* (${sites.length}):\n\nاضغط على أي موقع لتفعيله أو إيقافه.`,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: btns }
          });
          return;
        }

        // ── حظر / رفع حظر مستخدم ───────────────────────────────────
        // ── إيقاف/تشغيل مشروع من طرف الإدارة (يمنع المستخدم من التشغيل يدوياً) ──
        if (data.startsWith('admprojtoggle_')) {
          const projectId = data.replace('admprojtoggle_', '');
          const found = reqFindProjectById(projectId);
          if (!found) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          if (found.project.adminStopped) {
            found.project.adminStopped = false;
            found.project.active = true;
          } else {
            found.project.adminStopped = true;
            found.project.active = false;
          }
          reqSaveLocalData();
          await reqBackupToChannel();
          const statusIcon = found.project.adminStopped ? '⛔ موقوف من الإدارة' : '🟢 يعمل';
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `تم تحديث حالة *${found.project.name}*: ${statusIcon}`,
            parse_mode: 'Markdown'
          });
          // إشعار صاحب المشروع
          await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
            chat_id: found.ownerId,
            text: found.project.adminStopped
              ? `⛔ تم إيقاف مشروعك "${found.project.name}" من قبل الإدارة.`
              : `🟢 تم إعادة تشغيل مشروعك "${found.project.name}" من قبل الإدارة.`
          }).catch(() => {});
          return;
        }

        // ── تحديد/تعديل سعر مشروع "تحويل إلى دائم" ─────────────────
        if (data.startsWith('txnaddprice_') || data.startsWith('txneditprice_')) {
          const projectId = data.startsWith('txnaddprice_') ? data.replace('txnaddprice_', '') : data.replace('txneditprice_', '');
          const found = reqFindProjectById(projectId);
          if (!found) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع غير موجود.' });
            return;
          }
          reqAdminSessions[chatId] = { action: 'set_project_price', projectId };
          const currentPrice = found.project.permanentPrice ? `\n\nالسعر الحالي: ${found.project.permanentPrice}$` : '';
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: `💲 أرسل السعر الجديد (بالدولار) لمشروع "${found.project.name}":${currentPrice}\n\n(للإلغاء اكتب /cancel)`
          });
          return;
        }

        if (data.startsWith('admblock_')) {
          const targetId = data.replace('admblock_', '');
          const u = reqGetUser(targetId);
          if (!u) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المستخدم غير موجود.' });
            return;
          }
          u.blocked = !u.blocked;
          reqSaveLocalData();
          await reqBackupToChannel();
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: u.blocked ? `⛔ تم حظر ${reqUserDisplayName(u)}.` : `✅ تم رفع الحظر عن ${reqUserDisplayName(u)}.`,
            reply_markup: { inline_keyboard: [[{ text: '🔙 رجوع لملف المستخدم', callback_data: `admuser_${targetId}` }]] }
          });
          return;
        }

        // ── زر "الرد على هذا المستخدم" تحت رسالة ريلاي/استمرارية ────
        if (data.startsWith('relayreply_')) {
          const targetUserId = data.replace('relayreply_', '');
          reqAdminSessions[chatId] = { action: 'relay_reply', targetUserId };
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: '✏️ اكتب الرسالة التي تريد إرسالها لهذا المستخدم:\n\n(للإلغاء اكتب /cancel)'
          });
          return;
        }
      }

      // ─── message ──────────────────────────────────────────────────
      const msg  = update.message;
      if (!msg) return;
      const chatId = msg.chat.id;
      const text   = (msg.text || '').trim();

      // ══════════════════════════════════════════════════════════════
      // ═══ رسائل مستخدمين عاديين (غير مدراء) لبوت الأدمن ═════════════
      // ═══ نظام "جعل المشروع يعمل باستمرار" + الريلاي المستمر مع الدعم ═
      // ══════════════════════════════════════════════════════════════
      if (!reqIsAdmin(chatId)) {
        if (text === '/start' || text.startsWith('/start')) {
          delete reqRelayUserSessions[chatId];
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: 'أرسل الرسالة التي أعطاك إياها بوت الطلبات والخاصة بمشروعك كي أعرف المشروع الذي تريده أن يستمر بالعمل.'
          });
          return;
        }

        // إن كان النص معرّف استمرارية (keepAliveId) صالح
        const kaThread = text && requestAppData.keepAliveThreads && requestAppData.keepAliveThreads[text];
        if (kaThread) {
          if (String(kaThread.ownerChatId) !== String(chatId)) {
            await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ هذا المعرّف غير خاص بحسابك.' });
            return;
          }
          kaThread.active = true;
          reqSaveLocalData();
          reqRelayUserSessions[chatId] = { keepAliveId: text, projectName: kaThread.projectName };

          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId,
            text: 'سيرد عليك المدير في أقرب وقت كي يقول لك تكلفة المشروع وتكلفة استمراره 🙏'
          });

          // إرسال للإدارة مع زر "الرد على هذا المستخدم"
          const infoText = (
            `🔁 *طلب استمرارية مشروع*\n\n` +
            `👤 العميل: ${kaThread.ownerName} ${kaThread.ownerUsername}\n` +
            `🆔 ID: \`${chatId}\`\n` +
            `🏷 المشروع: ${kaThread.projectName} (${reqProjectTypeLabel(kaThread.projectType)})`
          );
          await reqNotifyAllAdmins({
            text: infoText,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '💬 الرد على هذا المستخدم', callback_data: `relayreply_${chatId}` }]] }
          });
          return;
        }

        // إن كان لدى هذا المستخدم جلسة ريلاي مفتوحة مسبقاً: أي رسالة تالية توصل تلقائياً للإدارة
        if (reqRelayUserSessions[chatId]) {
          const rs = reqRelayUserSessions[chatId];
          const senderName = `${msg.from.first_name || ''} ${msg.from.last_name || ''}`.trim() || 'عميل';
          const senderUname = msg.from.username ? `@${msg.from.username}` : '';
          const relayText = (
            `📩 *رسالة من العميل*\n\n` +
            `👤 ${senderName} ${senderUname}\n` +
            `🆔 ID: \`${chatId}\`\n` +
            `🏷 المشروع: ${rs.projectName}\n\n` +
            `💬 ${text || '(رسالة غير نصية)'}`
          );
          await reqNotifyAllAdmins({
            text: relayText,
            parse_mode: 'Markdown',
            reply_markup: { inline_keyboard: [[{ text: '💬 الرد على هذا المستخدم', callback_data: `relayreply_${chatId}` }]] }
          });
          return;
        }

        // لا توجد جلسة استمرارية ولا معرّف صالح: توجيه المستخدم
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: 'أرسل الرسالة التي أعطاك إياها بوت الطلبات والخاصة بمشروعك كي أعرف المشروع الذي تريده أن يستمر بالعمل.'
        });
        return;
      }

      // من هنا فصاعداً: المستخدم مؤكد أنه مدير

      if (text === '/start' || text === '/admin') {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '🛠 لوحة تحكم البوت الإداري\n\nاختر العملية:', ...reqBuildAdminMenuKeyboard() });
        return;
      }

      if (text === '/cancel') {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '❌ تم إلغاء العملية الحالية.', ...reqBuildAdminMenuKeyboard() });
        return;
      }

      // ═══ تنقّل لوحة المفاتيح الرئيسية ═══════════════════════════
      if (text === ADMIN_BTN_BACK) {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '🛠 القائمة الرئيسية:', ...reqBuildAdminMenuKeyboard() });
        return;
      }

      if (text === ADMIN_BTN_SITES) {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '🌐 إدارة المواقع:', ...reqBuildSitesMenuKeyboard() });
        return;
      }

      if (text === ADMIN_BTN_MESSAGES) {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✉️ إدارة الرسائل:', ...reqBuildMessagesMenuKeyboard() });
        return;
      }

      if (text === ADMIN_BTN_BACKUP) {
        delete reqAdminSessions[chatId];
        await reqBackupToChannel();
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم إرسال النسخة الاحتياطية.', ...reqBuildAdminMenuKeyboard() });
        return;
      }

      if (text === ADMIN_BTN_TRANSACTIONS) {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '💰 إدارة المعاملات النقدية:', ...reqBuildTransactionsMenuKeyboard() });
        return;
      }

      if (text === ADMIN_BTN_USERS) {
        delete reqAdminSessions[chatId];
        const users = Object.values(requestAppData.users || {});
        if (!users.length) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا يوجد مستخدمون مسجلون بعد.', ...reqBuildAdminMenuKeyboard() });
          return;
        }
        const btns = users.map(u => [{ text: `${u.blocked ? '⛔' : '👤'} ${reqUserDisplayName(u)}`, callback_data: `admuser_${u.chatId}` }]);
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: `👥 *كل المستخدمين* (${users.length}):\n\nاضغط على اسم المستخدم لعرض خياراته.`,
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: btns }
        });
        return;
      }

      // ═══ تنقّل لوحة مفاتيح "المواقع" ═════════════════════════════
      if (text === ADMIN_BTN_ORDERS_LIST) {
        const orders = requestAppData.orders || [];
        if (!orders.length) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا يوجد طلبات مسجلة بعد.', ...reqBuildSitesMenuKeyboard() });
          return;
        }
        const btns = orders.slice(0, 15).map((o, i) => [{
          text: `${i + 1}. ${o.categoryLabel} — ${o.clientName} (${o.dateStr})`,
          callback_data: `vieworder_${o.id}`
        }]);
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id:      chatId,
          text:         `📨 *قائمة الطلبات* — آخر ${Math.min(orders.length, 15)} طلب (المجموع: ${orders.length})`,
          parse_mode:   'Markdown',
          reply_markup: { inline_keyboard: btns }
        });
        return;
      }

      if (text === ADMIN_BTN_ALL_PROJECTS) {
        const all = requestAppData.confirmedSites || {};
        const allProjects = [];
        for (const uid of Object.keys(all)) {
          for (const p of (all[uid] || [])) allProjects.push({ ...p, ownerId: uid });
        }
        if (!allProjects.length) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا توجد مشاريع مسجلة بعد.', ...reqBuildSitesMenuKeyboard() });
          return;
        }
        const btns = allProjects.map(p => {
          const statusIcon = p.adminStopped ? '⛔' : (p.active === false ? '🔴' : '🟢');
          return [{ text: `${statusIcon} ${reqProjectTypeLabel(p.type)} — ${p.name} (${p.ownerId})`, callback_data: `admprojtoggle_${p.id}` }];
        });
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: '📦 *كل المشاريع:*\n\nاضغط على أي مشروع لإيقافه أو تشغيله من طرف الإدارة.',
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: btns }
        });
        return;
      }

      if (text === ADMIN_BTN_SITES_LIST) {
        if (!tempHostSites.size) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا يوجد مواقع مؤقتة مسجلة حالياً.', ...reqBuildSitesMenuKeyboard() });
          return;
        }
        const now  = Date.now();
        const btns = [];
        let listText = '🌐 *المواقع المؤقتة المسجّلة:*\n\n';
        let i = 1;
        for (const [slug, site] of tempHostSites.entries()) {
          const remain = Math.max(0, Math.floor((site.expiresAt - now) / 3600000));
          listText += `${i}. ${site.active ? '🟢' : '🔴'} ${site.label || slug} — يبقى ${remain} ساعة | 👤 ${site.ownerId}\n`;
          btns.push([{ text: `${site.active ? '🟢' : '🔴'} ${site.label || slug}`, callback_data: `admsite_toggle_${slug}` }]);
          i++;
        }
        listText += '\nاضغط على أي موقع لتفعيله أو إيقافه.';
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId, text: listText, parse_mode: 'Markdown', reply_markup: { inline_keyboard: btns }
        });
        return;
      }

      // ═══ تنقّل لوحة مفاتيح "الرسائل" ═════════════════════════════
      if (text === ADMIN_BTN_EDIT_WELCOME) {
        reqAdminSessions[chatId] = { action: 'edit_welcome' };
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✏️ اكتب رسالة الترحيب الجديدة:\n\n(للإلغاء اكتب /cancel)' });
        return;
      }

      if (text === ADMIN_BTN_EDIT_THANKS) {
        reqAdminSessions[chatId] = { action: 'edit_thanks' };
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✏️ اكتب رسالة الشكر الجديدة:\n\n(للإلغاء اكتب /cancel)' });
        return;
      }

      // ═══ تنقّل لوحة مفاتيح "معاملات نقدية" ═══════════════════════
      if (text === ADMIN_BTN_BINANCE_BALANCE) {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ جلب الرصيد من بينانس...' });
        const bal = await reqBinanceGetBalance();
        if (!bal.ok) {
          let msg;
          if (bal.error === 'missing_api_keys') {
            msg = '⚠️ مفاتيح Binance API غير مُعرّفة على السيرفر (BINANCE_API_KEY / BINANCE_API_SECRET).';
          } else if (bal.raw !== undefined) {
            const rawText = typeof bal.raw === 'string' ? bal.raw : JSON.stringify(bal.raw, null, 2);
            msg = `❌ رد بينانس الخام:\n\`\`\`\n${rawText.slice(0, 3500)}\n\`\`\``;
          } else {
            msg = `❌ تعذّر جلب الرصيد: ${bal.error}`;
          }
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: msg, parse_mode: 'Markdown', ...reqBuildTransactionsMenuKeyboard() });
          return;
        }
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: (
            `💵 *رصيد USDT في بينانس:*\n\n` +
            `▫️ المتاح: ${bal.free.toFixed(2)}$\n` +
            `▫️ محجوز: ${bal.locked.toFixed(2)}$\n` +
            `▫️ الإجمالي: *${bal.total.toFixed(2)}$*`
          ),
          parse_mode: 'Markdown',
          ...reqBuildTransactionsMenuKeyboard()
        });
        return;
      }

      if (text === ADMIN_BTN_LAST_TRANSFERS) {
        delete reqAdminSessions[chatId];
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ جلب آخر التحويلات...' });
        const res = await reqBinanceGetLastDeposits(10);
        if (!res.ok) {
          let msg;
          if (res.error === 'missing_api_keys') {
            msg = '⚠️ مفاتيح Binance API غير مُعرّفة على السيرفر (BINANCE_API_KEY / BINANCE_API_SECRET).';
          } else if (res.raw !== undefined) {
            const rawText = typeof res.raw === 'string' ? res.raw : JSON.stringify(res.raw, null, 2);
            msg = `❌ رد بينانس الخام:\n\`\`\`\n${rawText.slice(0, 3500)}\n\`\`\``;
          } else {
            msg = `❌ تعذّر جلب سجل التحويلات: ${res.error}`;
          }
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: msg, parse_mode: 'Markdown', ...reqBuildTransactionsMenuKeyboard() });
          return;
        }
        if (!res.deposits.length) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا يوجد أي عمليات إيداع مسجّلة بعد.', ...reqBuildTransactionsMenuKeyboard() });
          return;
        }
        const statusLabel = st => (st === 1 ? '✅ مؤكدة' : st === 0 ? '⏳ قيد التأكيد' : '❔ غير معروفة');
        let listText = `📜 *آخر ${res.deposits.length} تحويلات (USDT):*\n\n`;
        res.deposits.forEach((d, i) => {
          const date = d.insertTime ? new Date(d.insertTime).toLocaleString('ar-SY') : '—';
          listText += `${i + 1}. 💰 ${d.amount}$ | ${statusLabel(d.status)}\n📅 ${date}\n🔗 \`${d.txId || '—'}\`\n\n`;
        });
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: listText, parse_mode: 'Markdown', ...reqBuildTransactionsMenuKeyboard() });
        return;
      }

      if (text === ADMIN_BTN_ADD_PRICE || text === ADMIN_BTN_EDIT_PRICE) {
        delete reqAdminSessions[chatId];
        const all = requestAppData.confirmedSites || {};
        const allProjects = [];
        for (const uid of Object.keys(all)) {
          for (const p of (all[uid] || [])) allProjects.push({ ...p, ownerId: uid });
        }
        const isAdd = text === ADMIN_BTN_ADD_PRICE;
        const targetProjects = isAdd
          ? allProjects.filter(p => !p.permanentPrice)
          : allProjects.filter(p => !!p.permanentPrice);
        if (!targetProjects.length) {
          const emptyMsg = isAdd
            ? '📭 لا توجد مشاريع بلا سعر حالياً؛ كل المشاريع لديها سعر محدد بالفعل.'
            : '📭 لا توجد مشاريع لديها سعر محدد بعد لتعديله.';
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: emptyMsg, ...reqBuildTransactionsMenuKeyboard() });
          return;
        }
        const prefix = isAdd ? 'txnaddprice_' : 'txneditprice_';
        const btns = targetProjects.map(p => [{
          text: `${reqProjectTypeLabel(p.type)} — ${p.name}${p.permanentPrice ? ` (${p.permanentPrice}$)` : ''}`,
          callback_data: `${prefix}${p.id}`
        }]);
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: isAdd ? '➕ اختر المشروع الذي تريد تحديد سعره:' : '✏️ اختر المشروع الذي تريد تعديل سعره:',
          reply_markup: { inline_keyboard: btns }
        });
        return;
      }

      if (text === ADMIN_BTN_PAID_PROJECTS) {
        delete reqAdminSessions[chatId];
        const all = requestAppData.confirmedSites || {};
        const paid = [];
        for (const uid of Object.keys(all)) {
          for (const p of (all[uid] || [])) {
            if (p.permanentPaymentState === 'confirmed') paid.push({ ...p, ownerId: uid });
          }
        }
        if (!paid.length) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '📭 لا توجد مشاريع مدفوعة بعد.', ...reqBuildTransactionsMenuKeyboard() });
          return;
        }
        let listText = `✅ *المشاريع المدفوعة* (${paid.length}):\n\n`;
        paid.forEach((p, i) => {
          const date = p.permanentPaymentAt ? new Date(p.permanentPaymentAt).toLocaleString('ar-SY') : '—';
          listText += `${i + 1}. ${reqProjectTypeLabel(p.type)} — *${p.name}*\n👤 المالك: ${p.ownerId}\n💰 السعر: ${p.permanentPrice || '—'}$\n🔗 TxID: \`${p.permanentPaymentTxId || '—'}\`\n📅 ${date}\n\n`;
        });
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: listText, parse_mode: 'Markdown', ...reqBuildTransactionsMenuKeyboard() });
        return;
      }

      if (text.startsWith('/')) return;

      const session = reqAdminSessions[chatId];
      if (!session) return;

      if (session.action === 'edit_welcome') {
        requestAppData.welcome_text = text;
        await reqBackupToChannel();
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم تحديث رسالة الترحيب!', ...reqBuildMessagesMenuKeyboard() });
        delete reqAdminSessions[chatId];
        return;
      }

      if (session.action === 'set_project_price') {
        const priceVal = parseFloat(String(text).replace(',', '.').trim());
        if (isNaN(priceVal) || priceVal <= 0) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ يرجى إرسال رقم صحيح أكبر من صفر (مثال: 15 أو 15.5).' });
          return;
        }
        const found = reqFindProjectById(session.projectId);
        delete reqAdminSessions[chatId];
        if (!found) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ المشروع لم يعد موجوداً.', ...reqBuildTransactionsMenuKeyboard() });
          return;
        }
        found.project.permanentPrice = priceVal;
        reqSaveLocalData();
        await reqBackupToChannel();
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: `✅ تم تحديد سعر مشروع "${found.project.name}" بـ *${priceVal}$*`,
          parse_mode: 'Markdown',
          ...reqBuildTransactionsMenuKeyboard()
        });
        // إشعار صاحب المشروع بأن السعر جاهز الآن
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
          chat_id: found.ownerId,
          text: `💲 تم تحديد سعر تحويل مشروعك "${found.project.name}" إلى دائم: ${priceVal}$\n\nيمكنك المتابعة الآن عبر زر "تحويل مشروعي إلى دائم".`
        }).catch(() => {});
        return;
      }

      if (session.action === 'edit_thanks') {
        requestAppData.thank_you_text = text;
        await reqBackupToChannel();
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '✅ تم تحديث رسالة الشكر!', ...reqBuildMessagesMenuKeyboard() });
        delete reqAdminSessions[chatId];
        return;
      }

      // ── مراسلة مستخدم محدد: استلام نص الرسالة وإرسالها ────────────
      if (session.action === 'msg_user') {
        const targetId = session.targetId;
        const sent = await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
          chat_id: targetId,
          text: text
        }).catch(() => null);
        if (sent && sent.ok) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text: '✅ تم إرسال الرسالة للمستخدم.',
            reply_markup: { inline_keyboard: [[{ text: '🔙 رجوع لملف المستخدم', callback_data: `admuser_${targetId}` }]] }
          });
        } else {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر إرسال الرسالة. قد يكون المستخدم حظر البوت.' });
        }
        delete reqAdminSessions[chatId];
        return;
      }

      // ── رد مدير على مستخدم ضمن نظام "استمرارية المشروع" (ريلاي بوت الأدمن) ──
      if (session.action === 'relay_reply') {
        const targetUserId = session.targetUserId;
        const sent = await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: targetUserId,
          text: text
        }).catch(() => null);
        delete reqAdminSessions[chatId];
        if (sent && sent.ok) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
            chat_id: chatId, text: '✅ تم الإرسال.',
            reply_markup: { inline_keyboard: [[{ text: '💬 الرد على هذا المستخدم', callback_data: `relayreply_${targetUserId}` }]] }
          });
        } else {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ تعذّر إرسال الرسالة. قد يكون المستخدم لم يبدأ محادثة مع بوت الأدمن بعد.' });
        }
        return;
      }

      // ── تأكيد الطلب: استلام رابط الموقع وإرساله للمستخدم ─────────
      if (session.action === 'confirm_order_url') {
        const orderId = session.orderId;
        const order   = (requestAppData.orders || []).find(o => o.id === orderId);
        if (!order) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ الطلب غير موجود.' });
          delete reqAdminSessions[chatId];
          return;
        }
        const siteUrl = text.trim();
        session.action = 'confirm_order_index';
        session.siteUrl = siteUrl;
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: '📄 الآن أرسل ملف *index.html* الخاص بهذا المشروع (كمستند)، ليتم تخزينه في مجموعة "إنشاء تطبيقات ومواقع".',
          parse_mode: 'Markdown'
        });
        return;
      }

      // ── تأكيد الطلب: استلام ملف index.html، تخزينه، وإنهاء الطلب ──
      if (session.action === 'confirm_order_index') {
        if (!msg.document) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ أرسل ملف index.html كمستند (Document) وليس كنص.' });
          return;
        }
        const orderId = session.orderId;
        const order   = (requestAppData.orders || []).find(o => o.id === orderId);
        if (!order) {
          await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⚠️ الطلب غير موجود.' });
          delete reqAdminSessions[chatId];
          return;
        }
        const siteUrl = session.siteUrl;

        // إعادة رفع الملف عبر بوت الأدمن إلى مجموعة "إنشاء تطبيقات ومواقع" (نفس قناة الأرشيف الدائمة)
        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', { chat_id: chatId, text: '⏳ جارٍ تخزين الملف...' });
        const fwd = await reqSendToTg(REQUEST_ADMIN_BOT_API, 'forwardMessage', {
          chat_id: REQUEST_BACKUP_CHANNEL,
          from_chat_id: chatId,
          message_id: msg.message_id
        }).catch(e => { console.error('[ReqBot] فشل تخزين index.html:', e.message); return null; });

        const indexMsgId  = (fwd && fwd.ok) ? fwd.result.message_id : null;
        const indexFileId = msg.document.file_id;

        // تأكيد الطلب وحفظ الرابط
        order.confirmed    = true;
        order.confirmedAt  = new Date().toISOString();
        order.confirmedUrl = siteUrl;

        // إضافة المشروع لقائمة مشاريع المستخدم (بالنموذج الموحّد الجديد)
        const projectType = order.categoryKind === 'apk' ? 'apk' : (order.categoryKind === 'app' ? 'apk' : (order.categoryKind === 'bot' ? 'bot' : 'website'));
        reqAddUserProject(order.chatId, {
          type:        projectType,
          name:        order.categoryLabel,
          url:         siteUrl,
          indexFileId,
          indexMsgId
        });

        await reqBackupToChannel();

        // إرسال رسالة للمستخدم بالرابط فقط (بدون الملف)
        await reqSendToTg(REQUEST_MAIN_BOT_API, 'sendMessage', {
          chat_id: order.chatId,
          text: (
            `🎉 *تم إنجاز طلبك!*\n\n` +
            `🏷 ${order.categoryLabel}\n\n` +
            `🌐 رابط موقعك:\n${siteUrl}\n\n` +
            `يمكنك الوصول إليه دائماً من قسم *مشاريعي* في البوت. 🚀`
          ),
          parse_mode: 'Markdown'
        }).catch(e => console.error('[ReqBot] فشل إرسال التأكيد للعميل:', e.message));

        await reqSendToTg(REQUEST_ADMIN_BOT_API, 'sendMessage', {
          chat_id: chatId,
          text: `✅ تم تأكيد الطلب، تخزين index.html، وإرسال الرابط للعميل *${order.clientName}* بنجاح!`,
          parse_mode: 'Markdown',
          ...reqBuildSitesMenuKeyboard()
        });
        delete reqAdminSessions[chatId];
        return;
      }

    } catch(e) { console.error('[RequestBot Admin] خطأ:', e.message); }
  });
});

// ══════════════════════════════════════════════════════════════════════
// ═══ البوتات المُنشأة تلقائياً عبر تصنيف "إنشاء بوتات تلغرام" ═══════
// endpoint عام واحد يستقبل تحديثات كل البوتات المُدارة، كل بوت مميّز
// عبر :botId (= username البوت) في مسار الـ webhook الخاص به.
// ══════════════════════════════════════════════════════════════════════
app.post('/managed-bot/:botId', (req, res) => {
  res.sendStatus(200);
  setImmediate(async () => {
    try {
      const botId = req.params.botId;
      const cfg   = reqGetManagedBotConfig(botId);
      if (!cfg) return; // بوت غير معروف أو تم حذفه

      // ── التحقق من حالة تشغيل/إيقاف البوت (زر ⏸ إيقاف / 🟢 تشغيل في "مشاريعي"،
      // أو إيقاف من الإدارة) قبل الرد على أي تحديث ─────────────────────────
      const linkedProject = reqFindProjectByBotUsername(cfg.username);
      if (linkedProject && (linkedProject.adminStopped || linkedProject.active === false)) {
        return; // البوت موقوف حالياً، لا نرد على أي رسالة أو زر
      }

      const botApi  = 'https://api.telegram.org/bot' + cfg.token;
      const update  = req.body;
      if (!update) return;

      // ── ضغط زر Inline ──────────────────────────────────────────────
      if (update.callback_query) {
        const query  = update.callback_query;
        const chatId = query.message.chat.id;
        await fetch(botApi + '/answerCallbackQuery', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ callback_query_id: query.id })
        });

        if (!query.data || !query.data.startsWith('mbbtn_')) return;
        const btnId = query.data.replace('mbbtn_', '');
        const btn   = (cfg.buttons || []).find(b => b.id === btnId);
        if (!btn) return;

        await reqSendManagedBotButtonContent(botApi, chatId, btn);
        return;
      }

      // ── رسالة عادية (start أو زر Keyboard) ───────────────────────
      const msg = update.message;
      if (!msg) return;
      const chatId = msg.chat.id;
      const text   = (msg.text || '').trim();

      if (text === '/start') {
        await fetch(botApi + '/sendMessage', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: chatId, text: cfg.welcomeText || '👋 أهلاً بك!',
            ...reqBuildManagedBotButtonsMarkup(cfg.buttons, cfg.displayMode)
          })
        });
        return;
      }

      // زر Keyboard: نطابق النص المكتوب مع تسمية أحد الأزرار
      if (cfg.displayMode === 'keyboard') {
        const btn = (cfg.buttons || []).find(b => b.label === text);
        if (btn) await reqSendManagedBotButtonContent(botApi, chatId, btn);
      }
    } catch (e) {
      console.error('[ManagedBot] خطأ:', e.message);
    }
  });
});

// ── إرسال محتوى الزر (نص/رابط/صورة/PDF/APK) حسب نوعه ──────────────────
async function reqSendManagedBotButtonContent(botApi, chatId, btn) {
  try {
    if (btn.type === 'text') {
      await fetch(botApi + '/sendMessage', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, text: btn.content || btn.label })
      });
    } else if (btn.type === 'link') {
      await fetch(botApi + '/sendMessage', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, text: btn.content || '', disable_web_page_preview: false })
      });
    } else if (btn.type === 'photo' && btn.fileId) {
      await fetch(botApi + '/sendPhoto', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, photo: btn.fileId })
      });
    } else if ((btn.type === 'pdf' || btn.type === 'apk') && btn.fileId) {
      await fetch(botApi + '/sendDocument', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, document: btn.fileId })
      });
    } else {
      await fetch(botApi + '/sendMessage', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chatId, text: '⚠️ تعذّر عرض محتوى هذا الزر حالياً.' })
      });
    }
  } catch (e) {
    console.error('[ManagedBot] خطأ إرسال محتوى الزر:', e.message);
  }
}

// ══════════════════════════════════════════════════════════════════════
// ═══ Web Push Endpoints ═══════════════════════════════════════════════
// ══════════════════════════════════════════════════════════════════════

// GET /web-push-vapid-key — جلب المفتاح العام (لا يحتاج secret)
app.get('/web-push-vapid-key', (req, res) => {
  res.json({ publicKey: VAPID_PUBLIC_KEY });
});

// POST /web-push-subscribe — حفظ subscription جديد
app.post('/web-push-subscribe', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { uid, subscription } = req.body;
  if (!uid || !subscription) return res.json({ success: false, error: 'uid و subscription مطلوبان' });
  try {
    const subStr = JSON.stringify(subscription);
    const url  = `${FIRESTORE_BASE}/users/${uid}?key=${FIREBASE_API_KEY}&updateMask.fieldPaths=webPushSub`;
    const body = { fields: { webPushSub: { stringValue: subStr } } };
    await fetch(url, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    await addSubscriberUid(uid);
    if (!pushPollingActive) startWebPushPolling();
    console.log('[WebPush] Subscribed: ' + uid);
    res.json({ success: true });
  } catch (e) {
    console.error('[WebPush] subscribe error:', e.message);
    res.json({ success: false, error: e.message });
  }
});

// POST /web-push-unsubscribe — إلغاء subscription
app.post('/web-push-unsubscribe', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { uid } = req.body;
  if (!uid) return res.json({ success: false, error: 'uid مطلوب' });
  try {
    await removeSubscriberUid(uid);
    res.json({ success: true });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// POST /web-push-send — إرسال push يدوي للاختبار
app.post('/web-push-send', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { uid, title, body: msgBody, type } = req.body;
  if (!uid) return res.json({ success: false, error: 'uid مطلوب' });
  try {
    const sub = await getUserSubscription(uid);
    if (!sub) return res.json({ success: false, error: 'لا يوجد subscription لهذا المستخدم' });
    const result = await sendWebPush(sub, {
      type:  type  || 'general',
      title: title || 'اختبار كارو',
      body:  msgBody || 'هذا اشعار تجريبي'
    });
    res.json({ success: result === true, result });
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// POST /web-push-test-notif — كتابة إشعار تجريبي في pendingPushNotifs مباشرة
app.post('/web-push-test-notif', async (req, res) => {
  if (!checkSecret(req, res)) return;
  const { uid, title, body: msgBody, type } = req.body;
  if (!uid) return res.json({ success: false, error: 'uid مطلوب' });
  try {
    const docBody = {
      fields: {
        type:  { stringValue: type  || 'general' },
        title: { stringValue: title || 'اختبار كارو 🔔' },
        body:  { stringValue: msgBody || 'هذا إشعار تجريبي من السيرفر' },
        senderPhoto: { stringValue: '' },
        callId: { stringValue: '' }
      }
    };
    const url = `${FIRESTORE_BASE}/pendingPushNotifs/${uid}/queue?key=${FIREBASE_API_KEY}`;
    const r = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(docBody)
    });
    const data = await r.json();
    if (data.name) {
      res.json({ success: true, docId: data.name.split('/').pop(), message: 'تم كتابة الإشعار في Firestore، السيرفر سيرسله خلال 15 ثانية' });
    } else {
      res.json({ success: false, error: data });
    }
  } catch (e) {
    res.json({ success: false, error: e.message });
  }
});

// GET /web-push-status — حالة النظام
app.get('/web-push-status', async (req, res) => {
  if (!checkSecret(req, res)) return;
  try {
    const [uids, pendingUids] = await Promise.all([
      getSubscriberUids(),
      getAllPendingNotifUids()
    ]);
    res.json({
      polling: pushPollingActive,
      subscribers: uids.length,
      uids,
      pendingUids,
      pendingCount: pendingUids.length
    });
  } catch(e) {
    res.json({ polling: pushPollingActive, subscribers: 0, error: e.message });
  }
});


// ═══ استضافة HTML المؤقتة (تُستخدم من داخل بوت الإنشاء) ═════════════
// ═══ كل ملف index.html يُرفع يحصل على رابط دائم عبر /p/:slug ════════
// ══════════════════════════════════════════════════════════════════════

const TEMPHOST_BASE_URL   = "https://temporaryapplink.hafezalmahmoud095.workers.dev";

// ─── Firebase / Firestore للمشروع temphostsites ──────────────────────
const TH_SERVICE_ACCOUNT = {
  "type": "service_account",
  "project_id": "temphostsites",
  "private_key_id": "1692ff5ea2cd878a7e111ffd21ae8c78f2d9f94e",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCh65mB9LjkDz7g\nqhGlT+zGT8tRr6W9OopCZaMP7or5480ynZzQY0j/bod53ttbWL2vKO651614DYru\nCEPLyAiYgBQrploiUyyEba+bAVASak/MSUTgwuEmZTcFiK0ZzgboXXKCNn5HkTak\njeq0bIBdD8K14cx7VN8z9YfKESPU6zoOxzSq3Iai09eiBMRKj5i+mp3EVM6cUEEq\n0xmp7ACJqFDbw+T2P5kHhmGFSLG7fLt2x6AYvFLgHvfqRY0NRE79N/Fj375F6sVo\naXDd0VeSFKnx6OVYITRXKCoODz0MGC7QSCheBYkS72RSk/1HAVf3u9QPMbV4/UGG\nf/TaM6jJAgMBAAECggEABbNebjLdfOSvLrMOIqcpC4+s9YKc791AbOIDLFZrp4Hx\nAYKHDix7Wh2GwNsAbLuNF2j2Lq4nOf4lZrKnyw7kg+n5IShJgsKG42AGj2CBHy+F\nq7LmApEZ4tjWvRJ4ywXJPHkTGlPXfPw3KPKjfBgDKkOZyVQvzM/KI/GAXF7fchQ1\nAmeAQKWVSjJaVRDspTaIOWfR8ykLbNtYDHuTlFbhO625nj+MnU0xjTR/L2X562eJ\nUlNaEM0jG0T09Yx9csX8TkF5xR10DaOIB3ae2XwfRDqKCKE8t1iNvrfFGL0Gv8LM\nm0xcNaqw0f2ocgHwv9C9JKXAr8wsdR/cCd0MoxgTuQKBgQDUZoT7rigTLGa+kyie\n1EyTJ9BNuPmURz5Dn4M/O7raJU+iFTISwqfGm0e9aa8oJcOCM4k1XOXGB5jncPFe\n9SNyrXD0nCEuKlEjWLtjAYCNs33AJAODtb2XbF5BlAwG099eH+FDhoK+N9SNPnbh\npLkfseinpoZXgoVvDujKWZp2MwKBgQDDKGMQqJ9goBv4pqYbwcypupXeGatqyqqh\nxTIckPrB9Otv+wfZzLnmbHYSs0BVuounRFpCVKskSQVDKMQOOOJ16hjKyXU0thbl\naKoGc8Qq8wal/qWxv/ukZQk+l4MCgNVvyAnAANUjPq93Li5n/rlIdzWfoB+Kfq4i\nAzGNwA6REwKBgDPZKtE9PC8iAZq31YygCmlJqMGwS8x0b7CWObWv4PbrcLsCLY0C\n023Z7fNA3y8PuOAJsI7ENJrYs+ybV3B0qsiNqissCbV5QwE74dJTRYxRqnrCz9DR\nBoz2OTQM7bqk8bvKUHTpWvUQL+SiwOZoDYC9LyvtrJGkHOp1W0I6CUidAoGASVKw\nPb8M+nHNcJO0TzN2IMMAscy1Mc9pYpRDaqYyAJNzrcQERBl+MwvHJOmCH3OSKsss\nmkVTc7OXrY7wcmN++kx0+iPMzHwpiBHV58yxBG4ArndJa8o00qQ/X+vvBg6/olrn\nBuOJMM/Jfx6zwtDablaTqCCyGQfVhuPqR1bEYk8CgYBhqZTzOb33X2Gi4Yuzzis5\nul4WevId+Ebb6Zwt2WAPk6IMuaF5N6cDyY/9BbE4CBQmm5ZYibCccHrtmQWBe1Ht\nPhnW+LC+U1ieNlscP+UWYwIOidr3ow72CggiyhmCnbxBRkQIiY+IDPLEmWeL283F\njhzRInRyWINwJUKfiaCDIg==\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-fbsvc@temphostsites.iam.gserviceaccount.com",
  "token_uri": "https://oauth2.googleapis.com/token"
};

const TH_FIRESTORE_BASE = `https://firestore.googleapis.com/v1/projects/temphostsites/databases/(default)/documents`;

// ─── كاش Access Token لـ Firestore ──────────────────────────────────
let _thFsToken = null;
let _thFsTokenExp = 0;

async function thGetFsToken() {
  const now = Math.floor(Date.now() / 1000);
  if (_thFsToken && now < _thFsTokenExp - 300) return _thFsToken;
  const crypto = require('crypto');
  const header  = Buffer.from(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).toString('base64url');
  const payload = Buffer.from(JSON.stringify({
    iss:   TH_SERVICE_ACCOUNT.client_email,
    scope: 'https://www.googleapis.com/auth/datastore',
    aud:   TH_SERVICE_ACCOUNT.token_uri,
    iat:   now,
    exp:   now + 3600
  })).toString('base64url');
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(`${header}.${payload}`);
  const sig = sign.sign(TH_SERVICE_ACCOUNT.private_key, 'base64url');
  const jwt = `${header}.${payload}.${sig}`;
  const r = await fetch(TH_SERVICE_ACCOUNT.token_uri, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`
  });
  const json = await r.json();
  if (!json.access_token) throw new Error('[TH-FS] token error: ' + JSON.stringify(json));
  _thFsToken = json.access_token;
  _thFsTokenExp = now + (json.expires_in || 3600);
  return _thFsToken;
}

// ─── تحويل بيانات Firestore لـ JS عادي ──────────────────────────────
function thFromFs(doc) {
  if (!doc || !doc.fields) return null;
  const out = {};
  for (const [k, v] of Object.entries(doc.fields)) {
    if (v.stringValue  !== undefined) out[k] = v.stringValue;
    else if (v.integerValue !== undefined) out[k] = Number(v.integerValue);
    else if (v.booleanValue !== undefined) out[k] = v.booleanValue;
    else if (v.doubleValue  !== undefined) out[k] = v.doubleValue;
  }
  return out;
}

// ─── تحويل JS عادي لصيغة Firestore ─────────────────────────────────
function thToFs(obj) {
  const fields = {};
  for (const [k, v] of Object.entries(obj)) {
    if (typeof v === 'string')  fields[k] = { stringValue: v };
    else if (typeof v === 'boolean') fields[k] = { booleanValue: v };
    else if (typeof v === 'number' && Number.isInteger(v)) fields[k] = { integerValue: String(v) };
    else if (typeof v === 'number') fields[k] = { doubleValue: v };
  }
  return { fields };
}

// ─── حفظ موقع في Firestore ──────────────────────────────────────────
async function thSaveToFirestore(slug, site) {
  try {
    const token = await thGetFsToken();
    const data = {
      slug,
      html:      site.html,
      ownerId:   String(site.ownerId),
      label:     site.label || slug,
      active:    site.active,
      createdAt: site.createdAt,
      expiresAt: site.expiresAt
    };
    const url = `${TH_FIRESTORE_BASE}/temphost_sites/${slug}`;
    const r = await fetch(url, {
      method: 'PATCH',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify(thToFs(data))
    });
    if (r.ok) console.log(`[TempHost] 💾 حُفظ ${slug} في Firestore`);
    else console.error('[TempHost] ❌ خطأ Firestore حفظ:', await r.text());
  } catch (e) { console.error('[TempHost] ❌ خطأ Firestore:', e.message); }
}

// ─── تحديث حقل في Firestore (active مثلاً) ──────────────────────────
async function thUpdateFirestore(slug, fields) {
  try {
    const token = await thGetFsToken();
    const updateMask = Object.keys(fields).map(k => `updateMask.fieldPaths=${k}`).join('&');
    const url = `${TH_FIRESTORE_BASE}/temphost_sites/${slug}?${updateMask}`;
    await fetch(url, {
      method: 'PATCH',
      headers: { 'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json' },
      body: JSON.stringify(thToFs(fields))
    });
  } catch (e) { console.error('[TempHost] ❌ خطأ تحديث Firestore:', e.message); }
}

// ─── حذف موقع من Firestore ──────────────────────────────────────────
async function thDeleteFromFirestore(slug) {
  try {
    const token = await thGetFsToken();
    await fetch(`${TH_FIRESTORE_BASE}/temphost_sites/${slug}`, {
      method: 'DELETE',
      headers: { 'Authorization': 'Bearer ' + token }
    });
    console.log(`[TempHost] 🗑️ حُذف ${slug} من Firestore`);
  } catch (e) { console.error('[TempHost] ❌ خطأ حذف Firestore:', e.message); }
}

// ─── استرجاع كل المواقع من Firestore عند بدء السيرفر ───────────────
async function thRestoreFromFirestore() {
  try {
    console.log('[TempHost] 🔄 استرجاع المواقع من Firestore...');
    const token = await thGetFsToken();
    const r = await fetch(`${TH_FIRESTORE_BASE}/temphost_sites`, {
      headers: { 'Authorization': 'Bearer ' + token }
    });
    if (!r.ok) { console.log('[TempHost] ℹ️ لا يوجد مواقع محفوظة.'); return; }
    const json = await r.json();
    if (!json.documents) { console.log('[TempHost] ℹ️ القاعدة فارغة.'); return; }
    const now = Date.now();
    let restored = 0;
    for (const doc of json.documents) {
      const data = thFromFs(doc);
      if (!data || !data.slug) continue;
      if (data.expiresAt < now) {
        thDeleteFromFirestore(data.slug).catch(() => {});
        continue;
      }
      tempHostSites.set(data.slug, {
        html:      data.html,
        ownerId:   data.ownerId,
        label:     data.label,
        active:    data.active,
        createdAt: data.createdAt,
        expiresAt: data.expiresAt
      });
      restored++;
    }
    console.log(`[TempHost] ✅ تم استرجاع ${restored} موقع من Firestore.`);
  } catch (e) {
    console.error('[TempHost] ⚠️ خطأ في الاسترجاع:', e.message);
  }
}

// ─── تخزين المواقع المؤقتة في الذاكرة ──────────────────────────────
const tempHostSites = new Map();

// ─── توليد slug عشوائي 8 أحرف ───────────────────────────────────────
function tempHostGenSlug() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let s = '';
  for (let i = 0; i < 8; i++) s += chars[Math.floor(Math.random() * chars.length)];
  return s;
}

// ─── بناء قائمة المواقع لمستخدم معين (يستخدمها بوت الطلبات الإداري) ──
function thBuildSiteList(ownerId) {
  const sites = [];
  for (const [slug, site] of tempHostSites.entries()) {
    if (String(site.ownerId) === String(ownerId)) {
      sites.push({ slug, ...site });
    }
  }
  return sites;
}

// ─── رفع محتوى HTML وإنشاء رابط له (يُستخدم من بوت الإنشاء) ─────────
async function thCreateSiteFromHtml(htmlContent, ownerId, label) {
  let slug;
  do { slug = tempHostGenSlug(); } while (tempHostSites.has(slug));

  const now       = Date.now();
  const expiresAt = now + (4 * 24 * 60 * 60 * 1000); // 4 أيام

  const newSite = {
    html:      htmlContent,
    createdAt: now,
    expiresAt,
    active:    true,
    ownerId:   String(ownerId),
    label:     label || slug
  };
  tempHostSites.set(slug, newSite);
  thSaveToFirestore(slug, newSite).catch(() => {});

  return { slug, link: `${TEMPHOST_BASE_URL}/p/${slug}`, expiresAt };
}

// ─── Endpoint تقديم الموقع المؤقت للزوار ────────────────────────
app.get('/p/:slug', (req, res) => {
  const slug = req.params.slug;
  const site = tempHostSites.get(slug);

  if (!site) {
    return res.status(404).send(`<!DOCTYPE html><html lang="ar" dir="rtl">
<head><meta charset="UTF-8"><title>404 - غير موجود</title>
<style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#f5f5f5;}
.box{text-align:center;padding:40px;background:#fff;border-radius:12px;box-shadow:0 2px 20px rgba(0,0,0,.1);}
h1{color:#e53935;font-size:3rem;margin:0;}p{color:#666;}</style></head>
<body><div class="box"><h1>404</h1><p>هذا الموقع غير موجود أو انتهت صلاحيته.</p></div></body></html>`);
  }

  if (Date.now() > site.expiresAt) {
    tempHostSites.delete(slug);
    return res.status(410).send(`<!DOCTYPE html><html lang="ar" dir="rtl">
<head><meta charset="UTF-8"><title>انتهت الصلاحية</title>
<style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#f5f5f5;}
.box{text-align:center;padding:40px;background:#fff;border-radius:12px;box-shadow:0 2px 20px rgba(0,0,0,.1);}
h1{color:#ff9800;font-size:2rem;}p{color:#666;}</style></head>
<body><div class="box"><h1>⏰ انتهت صلاحية هذا الرابط</h1><p>تم إيقاف هذا الموقع المؤقت.</p></div></body></html>`);
  }

  if (!site.active) {
    return res.status(503).send(`<!DOCTYPE html><html lang="ar" dir="rtl">
<head><meta charset="UTF-8"><title>موقع متوقف</title>
<style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;background:#f5f5f5;}
.box{text-align:center;padding:40px;background:#fff;border-radius:12px;box-shadow:0 2px 20px rgba(0,0,0,.1);}
h1{color:#9e9e9e;font-size:2rem;}p{color:#666;}</style></head>
<body><div class="box"><h1>🔴 هذا الموقع متوقف مؤقتاً</h1><p>يُرجى المحاولة لاحقاً.</p></div></body></html>`);
  }

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(site.html);
});

// ══════════════════════════════════════════════════════════════════════
// ═══ APK BUILDER BOT — بوت بناء تطبيقات APK ═════════════════════════
// ══════════════════════════════════════════════════════════════════════
//
// التدفق:
// /start → اختيار المصدر: ملف index.html أو رابط جاهز
//   - index.html → رفع الملف → إنشاء رابط (نفس منطق TempHost) → عرضه
//   - رابط جاهز → استلام الرابط مباشرة
// بعد توفر الرابط → سؤال: تريد بناء APK WebView؟ (نعم/إلغاء)
//   نعم → اسم التطبيق → الحزمة → الإصدار → رابط الأيقونة
//        → إخفاء شريط الحالة؟ → إخفاء الأزرار السفلية/الإيماءات؟
//        → هل لديك google-services.json وملف keystore؟ (نعم/لا)
//          نعم → رفع الملفين + كلمة سر/alias الـ keystore
//        → الأذونات (كل إذن بسؤال مستقل: كاميرا، مايكروفون، موقع،
//          تخزين، إشعارات، جهات اتصال، مكالمات)
//        → إطلاق GitHub Actions بكل المدخلات
// ══════════════════════════════════════════════════════════════════════

const APK_BOT_TOKEN    = process.env.APK_BOT_TOKEN;
const APK_BOT_API      = "https://api.telegram.org/bot" + APK_BOT_TOKEN;
const APK_GITHUB_OWNER = "supportcaro-crypto";
const APK_GITHUB_REPO  = "apk-builder";
const APK_GITHUB_TOKEN = GITHUB_TOKEN;

// ─── حالات المستخدمين (محادثة متعددة الخطوات) ───────────────────────
const apkUserStates = new Map();

// ══════════════════════════════════════════════════════════════════════
// ═══ التطبيق المؤقت (Temporary App) — package + google-services + ═══
// ═══ keystore موحّدين يُستخدمون لكل بناء "تطبيق مؤقت"             ═══
// ══════════════════════════════════════════════════════════════════════
// package name ثابت دائماً للتطبيق المؤقت (مسجّل بفايربيس مسبقاً)
const APK_TEMP_PACKAGE = "com.temporaryapp.app";

// ⚠️ هذه القيم تبقى فارغة حتى أول بناء ناجح لتطبيق مؤقت.
// بعد أول بناء، يصلك ملف الـ keystore + كلمة السر + الـ alias + SHA-1
// عبر بوت الإنشاء — عندها تحط القيم هون يدوياً (مرة وحدة فقط) ولن
// تحتاج لتكرار هذه الخطوة بعدها أبداً.
// ⚠️ مهم: بما أن الـ repo "apk-builder" خاص (private)، نخزن هون
// "المسار" داخل الـ repo فقط (وليس رابط raw مؤقت بتوكن منتهي الصلاحية).
// الـ workflow يحمّل الملف عبر GitHub API باستخدام التوكن وقت البناء.
const APK_TEMP_GOOGLE_SERVICES_PATH = "uploads/1782801505151-google-services.json";
const APK_TEMP_KEYSTORE_PATH        = "uploads/1782801446534-release.keystore";
const APK_TEMP_KEYSTORE_PASSWORD    = process.env.APK_TEMP_KEYSTORE_PASSWORD;
const APK_TEMP_KEYSTORE_ALIAS       = "builderapk";

// هل القيم أعلاه مكتملة؟ (تُستخدم لتحديد إذا كان هذا "أول بناء")
function apkTempAssetsReady() {
  return !!(APK_TEMP_GOOGLE_SERVICES_PATH && APK_TEMP_KEYSTORE_PATH &&
            APK_TEMP_KEYSTORE_PASSWORD && APK_TEMP_KEYSTORE_ALIAS);
}

// ─── قائمة الأذونات القابلة للاختيار (مفتاح → نص عرض) ───────────────
const APK_PERMISSIONS = [
  { key: 'camera',        label: '📷 الكاميرا' },
  { key: 'microphone',    label: '🎙️ المايكروفون' },
  { key: 'location',      label: '📍 الموقع الجغرافي' },
  { key: 'storage',       label: '🗂️ التخزين/الصور' },
  { key: 'notifications', label: '🔔 الإشعارات' },
  { key: 'contacts',      label: '👥 جهات الاتصال' },
  { key: 'call_phone',    label: '📞 الاتصال المباشر' }
];

// ─── helper: إرسال رسالة ─────────────────────────────────────────────
async function apkSend(chatId, text, extra = {}) {
  return fetch(`${APK_BOT_API}/sendMessage`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'HTML', ...extra })
  }).then(r => r.json()).catch(() => null);
}

// ─── helper: تعديل رسالة ─────────────────────────────────────────────
async function apkEdit(chatId, msgId, text, extra = {}) {
  return fetch(`${APK_BOT_API}/editMessageText`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ chat_id: chatId, message_id: msgId, text, parse_mode: 'HTML', ...extra })
  }).then(r => r.json()).catch(() => null);
}

// ─── helper: رفع ملف (أي نوع) إلى GitHub والحصول على رابط تنزيل ─────
async function apkUploadFileToGithub(fileBuffer, filename) {
  const content = fileBuffer.toString('base64');
  const path    = `uploads/${Date.now()}-${filename}`;
  const r = await fetch(
    `https://api.github.com/repos/${APK_GITHUB_OWNER}/${APK_GITHUB_REPO}/contents/${path}`,
    {
      method: 'PUT',
      headers: {
        'Authorization': `token ${APK_GITHUB_TOKEN}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ message: `Upload ${filename}`, content })
    }
  );
  if (!r.ok) throw new Error('فشل رفع الملف على GitHub');
  const data = await r.json();
  return data.content.download_url;
}

// ─── helper: جلب ملف من تيليجرام كـ Buffer ───────────────────────────
async function apkFetchTelegramFile(fileId) {
  const fileR = await fetch(`${APK_BOT_API}/getFile?file_id=${fileId}`);
  const fileD = await fileR.json();
  if (!fileD.ok) throw new Error('فشل جلب الملف من تيليجرام');
  const fileUrl = `https://api.telegram.org/file/bot${APK_BOT_TOKEN}/${fileD.result.file_path}`;
  const fileRes = await fetch(fileUrl);
  return { buffer: Buffer.from(await fileRes.arrayBuffer()), filePath: fileD.result.file_path };
}

// ─── helper: تشغيل GitHub Actions ────────────────────────────────────
async function apkTriggerBuild(state, chatId) {
  // ── بناء روابط Contents API الثابتة (تعمل مع private repo عبر التوكن) ──
  // لا نستخدم رابط raw المؤقت (ينتهي خلال دقائق)، بل رابط الـ
  // Contents API الذي يبقى صالحاً طالما الملف موجود بالـ repo، ويُحمَّل
  // داخل الـ workflow عبر header مصادقة بالتوكن.
  const googleServicesApiUrl = state.googleServicesPath
    ? `https://api.github.com/repos/${APK_GITHUB_OWNER}/${APK_GITHUB_REPO}/contents/${state.googleServicesPath}`
    : (state.googleServicesUrl || '');
  const keystoreApiUrl = state.keystorePath
    ? `https://api.github.com/repos/${APK_GITHUB_OWNER}/${APK_GITHUB_REPO}/contents/${state.keystorePath}`
    : (state.keystoreUrl || '');

  const inputs = {
    chat_id:             String(chatId),
    source_url:          state.sourceUrl,
    bot_token:           APK_BOT_TOKEN,
    app_name:            state.appName,
    app_package:         state.appPackage,
    app_version:         state.appVersion,
    icon_zip_url:        state.iconZipUrl,
    hide_status_bar:     state.hideStatusBar ? 'true' : 'false',
    hide_nav_bar:        state.hideNavBar ? 'true' : 'false',
    has_google_services: state.hasGoogleFiles ? 'true' : 'false',
    google_services_url: googleServicesApiUrl,
    has_keystore:        state.hasGoogleFiles ? 'true' : 'false',
    keystore_url:        keystoreApiUrl,
    keystore_password:   state.keystorePassword || '',
    keystore_alias:      state.keystoreAlias || '',
    permissions_csv:     (state.permissions || []).join(','),
    server_url:          'https://mahmoud08808665888888m-my-bot.hf.space',
    is_first_temp_build: state.isFirstTempBuild ? 'true' : 'false'
  };

  const r = await fetch(
    `https://api.github.com/repos/${APK_GITHUB_OWNER}/${APK_GITHUB_REPO}/actions/workflows/build-apk.yml/dispatches`,
    {
      method: 'POST',
      headers: {
        'Authorization': `token ${APK_GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github.v3+json',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ ref: 'main', inputs })
    }
  );
  return r.status === 204;
}

// ─── بناء كيبورد اختيار المصدر ───────────────────────────────────────
function apkSourceKeyboard() {
  return { reply_markup: { inline_keyboard: [
    [{ text: '📄 ملف index.html', callback_data: 'apk_src_html' }],
    [{ text: '🔗 رابط جاهز', callback_data: 'apk_src_url' }],
    [{ text: '♻️ تحديث index.html', callback_data: 'apk_update_html' }]
  ]}};
}

// ─── بناء كيبورد نعم/إلغاء ───────────────────────────────────────────
function apkYesNoKeyboard(yesData, noData) {
  return { reply_markup: { inline_keyboard: [
    [
      { text: '✅ نعم', callback_data: yesData },
      { text: '❌ إلغاء', callback_data: noData }
    ]
  ]}};
}

// ─── بناء كيبورد اختيار نوع التطبيق: مؤقت أو دائم ────────────────────
function apkTypeKeyboard() {
  return { reply_markup: { inline_keyboard: [
    [{ text: '⏳ تطبيق مؤقت', callback_data: 'apk_type_temp' }],
    [{ text: '🏢 تطبيق دائم', callback_data: 'apk_type_permanent' }]
  ]}};
}

// ─── عرض سؤال الإذن الحالي (واحد تلو الآخر) ──────────────────────────
async function apkAskNextPermission(chatId, msgIdToEdit = null) {
  const state = apkUserStates.get(chatId);
  const idx = state.permIndex || 0;

  if (idx >= APK_PERMISSIONS.length) {
    // انتهت الأذونات → إطلاق البناء
    await apkSend(chatId,
      `🚀 <b>جاري إطلاق البناء...</b>\n\n` +
      `📱 ${state.appName}\n` +
      `📦 ${state.appPackage}\n` +
      `🔢 v${state.appVersion}\n\n` +
      `⏳ سيصلك APK خلال 10-15 دقيقة.`
    );
    if (state.isFirstTempBuild) {
      await apkSend(chatId,
        `⚠️ <b>تنويه مهم:</b> هذا أول بناء لتطبيق مؤقت، لذا سيصلك بعد الـ APK:\n` +
        `🔑 ملف الـ keystore\n` +
        `🔒 كلمة السر والـ alias\n` +
        `🔐 بصمتي SHA-1 و SHA-256\n\n` +
        `سجّل SHA-1 في Firebase Console تحت تطبيق <code>${APK_TEMP_PACKAGE}</code> لتفعيل تسجيل الدخول عبر جوجل، ثم أرسل لي القيم الأربع (الملف، كلمة السر، الـ alias) عبر الأمر /save_temp_keystore لأخزنها وأستخدمها تلقائياً بكل التطبيقات المؤقتة القادمة.`
      );
    }
    const ok = await apkTriggerBuild(state, chatId);
    if (!ok) await apkSend(chatId, '❌ فشل إطلاق البناء. حاول مجدداً لاحقاً.');
    apkUserStates.delete(chatId);
    return;
  }

  const perm = APK_PERMISSIONS[idx];
  const text = `🔐 <b>الأذونات (${idx + 1}/${APK_PERMISSIONS.length})</b>\n\nهل يحتاج التطبيق إذن: ${perm.label}؟`;
  const kb = apkYesNoKeyboard(`apk_perm_yes_${perm.key}`, `apk_perm_no_${perm.key}`);

  if (msgIdToEdit) await apkEdit(chatId, msgIdToEdit, text, kb);
  else await apkSend(chatId, text, kb);
}

// ─── Webhook endpoint ─────────────────────────────────────────────────
app.post('/apkbuilder-webhook', async (req, res) => {
  res.sendStatus(200);
  try {
    const update = req.body;
    const ALLOWED_CHAT_ID = '6245764342';
    const incomingChatId = (update && update.callback_query && update.callback_query.message && update.callback_query.message.chat)
      ? update.callback_query.message.chat.id.toString()
      : (update && update.message && update.message.chat)
        ? update.message.chat.id.toString()
        : null;
    if (incomingChatId !== null && incomingChatId !== ALLOWED_CHAT_ID) return;

    // ════════════════════════════════════════════════════════════════
    // معالجة الأزرار (callback_query)
    // ════════════════════════════════════════════════════════════════
    if (update.callback_query) {
      const cb     = update.callback_query;
      const chatId = cb.message.chat.id;
      const msgId  = cb.message.message_id;
      const data   = cb.data;

      await fetch(`${APK_BOT_API}/answerCallbackQuery`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ callback_query_id: cb.id })
      });

      // ── اختيار المصدر: ملف html ──
      if (data === 'apk_src_html') {
        apkUserStates.set(chatId, { step: 'wait_html' });
        await apkEdit(chatId, msgId,
          '📄 <b>ملف index.html</b>\n\nأرسل ملف <code>index.html</code> الآن وسأعطيك رابطاً له.'
        );
        return;
      }

      // ── اختيار المصدر: رابط جاهز ──
      if (data === 'apk_src_url') {
        apkUserStates.set(chatId, { step: 'wait_ready_url' });
        await apkEdit(chatId, msgId,
          '🔗 <b>رابط جاهز</b>\n\nأرسل الرابط الآن:\n<i>مثال: https://example.com</i>'
        );
        return;
      }

      // ── زر: تحديث index.html — عرض قائمة كل المواقع المستضافة (من كل المستخدمين) ──
      if (data === 'apk_update_html' || data.startsWith('apk_updlist_')) {
        const page = data.startsWith('apk_updlist_') ? parseInt(data.split('_').pop(), 10) || 0 : 0;
        const now = Date.now();
        const allSites = [...tempHostSites.entries()]
          .filter(([, site]) => site.active && site.expiresAt > now)
          .sort((a, b) => b[1].createdAt - a[1].createdAt);

        if (allSites.length === 0) {
          await apkEdit(chatId, msgId, '📭 لا توجد مواقع مستضافة حالياً.');
          return;
        }

        const PAGE_SIZE = 8;
        const totalPages = Math.ceil(allSites.length / PAGE_SIZE);
        const pageSites = allSites.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

        const rows = pageSites.map(([slug, site]) => ([{
          text: `🌐 ${site.label || slug}`,
          callback_data: `apk_updpick_${slug}`
        }]));

        const navRow = [];
        if (page > 0) navRow.push({ text: '◀️ السابق', callback_data: `apk_updlist_${page - 1}` });
        if (page < totalPages - 1) navRow.push({ text: 'التالي ▶️', callback_data: `apk_updlist_${page + 1}` });
        if (navRow.length) rows.push(navRow);

        const listText = pageSites.map(([slug, site]) =>
          `🌐 <b>${site.label || slug}</b>\n${TEMPHOST_BASE_URL}/p/${slug}`
        ).join('\n\n');

        await apkEdit(chatId, msgId,
          `♻️ <b>اختر الموقع المراد تحديث ملفه:</b>\n\n${listText}\n\n📄 صفحة ${page + 1} من ${totalPages}`,
          { reply_markup: { inline_keyboard: rows } }
        );
        return;
      }

      // ── اختيار موقع محدد للتحديث ──
      if (data.startsWith('apk_updpick_')) {
        const slug = data.replace('apk_updpick_', '');
        const site = tempHostSites.get(slug);
        if (!site) {
          await apkEdit(chatId, msgId, '⚠️ هذا الموقع لم يعد موجوداً.');
          return;
        }
        apkUserStates.set(chatId, { step: 'wait_update_html_file', updateSlug: slug });
        await apkEdit(chatId, msgId,
          `♻️ <b>تحديث: ${site.label || slug}</b>\n${TEMPHOST_BASE_URL}/p/${slug}\n\n` +
          `أرسل الآن ملف <code>index.html</code> الجديد ليحل محل المحتوى الحالي على نفس الرابط (تاريخ الانتهاء الأصلي يبقى بدون تغيير).`
        );
        return;
      }

      // ── سؤال: تريد بناء APK WebView؟ → نعم ──
      if (data === 'apk_build_yes') {
        const state = apkUserStates.get(chatId);
        if (!state || !state.sourceUrl) {
          await apkSend(chatId, '⚠️ حدث خطأ، أرسل /start من جديد.');
          return;
        }
        await apkEdit(chatId, msgId,
          '🧩 <b>هل تريد إنشاء تطبيق مؤقت أم تطبيق دائم؟</b>\n\n' +
          '⏳ <b>مؤقت</b>: يستخدم نفس حزمة فايربيس والتوقيع المشترك تلقائياً (أسرع، بدون رفع ملفات).\n' +
          '🏢 <b>دائم</b>: مشروع فايربيس وتوقيع خاص بك (تحتاج لرفع ملفاتك الخاصة).',
          apkTypeKeyboard()
        );
        return;
      }

      // ── اختيار: تطبيق مؤقت ──
      if (data === 'apk_type_temp') {
        const state = apkUserStates.get(chatId);
        if (!state) return;
        state.appType    = 'temp';
        state.appPackage = APK_TEMP_PACKAGE;

        if (apkTempAssetsReady()) {
          // ── الأصول الموحدة جاهزة مسبقاً: استخدمها مباشرة (عبر مسارات الـ repo الخاص) ──
          state.hasGoogleFiles     = true;
          state.googleServicesPath = APK_TEMP_GOOGLE_SERVICES_PATH;
          state.keystorePath       = APK_TEMP_KEYSTORE_PATH;
          state.keystorePassword   = APK_TEMP_KEYSTORE_PASSWORD;
          state.keystoreAlias      = APK_TEMP_KEYSTORE_ALIAS;
          state.isFirstTempBuild   = false;
          await apkEdit(chatId, msgId,
            `✅ <b>تطبيق مؤقت</b> — سيتم استخدام حزمة <code>${APK_TEMP_PACKAGE}</code> وملفات فايربيس/التوقيع المشتركة تلقائياً.`
          );
        } else {
          // ── أول بناء على الإطلاق: لا توجد أصول موحدة بعد ──
          state.hasGoogleFiles   = false;
          state.isFirstTempBuild = true;
          await apkEdit(chatId, msgId,
            `✅ <b>تطبيق مؤقت</b> — حزمة <code>${APK_TEMP_PACKAGE}</code>.\n\n` +
            `⚠️ هذا أول بناء لتطبيق مؤقت، لذا سيتولّد مفتاح توقيع (keystore) جديد تلقائياً. ` +
            `سيصلك الملف مع كلمة السر والـ SHA-1 منفصلاً عن الـ APK — احتفظ فيه جيداً، فهو سيُستخدم لكل التطبيقات المؤقتة القادمة.`
          );
        }

        state.step = 'wait_app_name';
        await apkSend(chatId, '📱 <b>ما هو اسم التطبيق؟</b>');
        return;
      }

      // ── اختيار: تطبيق دائم ──
      if (data === 'apk_type_permanent') {
        const state = apkUserStates.get(chatId);
        if (!state) return;
        state.appType = 'permanent';
        state.step    = 'wait_app_name';
        await apkEdit(chatId, msgId,
          '🏢 <b>تطبيق دائم</b> — ستحتاج لرفع حزمتك الخاصة وملفات فايربيس والتوقيع لاحقاً.'
        );
        await apkSend(chatId, '📱 <b>ما هو اسم التطبيق؟</b>');
        return;
      }

      // ── سؤال: تريد بناء APK WebView؟ → إلغاء ──
      if (data === 'apk_build_no') {
        apkUserStates.delete(chatId);
        await apkEdit(chatId, msgId, '✅ تم الإلغاء. أرسل /start للبدء من جديد متى أردت.');
        return;
      }

      // ── هل لديك google-services.json و keystore؟ ──
      if (data === 'apk_gfiles_yes') {
        const state = apkUserStates.get(chatId);
        state.hasGoogleFiles = true;
        state.step = 'wait_google_services_file';
        await apkEdit(chatId, msgId, '📤 أرسل ملف <code>google-services.json</code> الآن.');
        return;
      }
      if (data === 'apk_gfiles_no') {
        const state = apkUserStates.get(chatId);
        state.hasGoogleFiles = false;
        state.step = null;
        state.permIndex = 0;
        await apkEdit(chatId, msgId, '✅ تمام، سيُبنى التطبيق بدون خدمات جوجل (WebView بسيط).');
        await apkAskNextPermission(chatId);
        return;
      }

      // ── إخفاء شريط الحالة ──
      if (data === 'apk_statusbar_yes' || data === 'apk_statusbar_no') {
        const state = apkUserStates.get(chatId);
        state.hideStatusBar = (data === 'apk_statusbar_yes');
        await apkEdit(chatId, msgId,
          '🧭 <b>هل تريد إخفاء الأزرار السفلية أو الإيماءات؟</b>',
          apkYesNoKeyboard('apk_navbar_yes', 'apk_navbar_no')
        );
        return;
      }

      // ── إخفاء الأزرار السفلية/الإيماءات ──
      if (data === 'apk_navbar_yes' || data === 'apk_navbar_no') {
        const state = apkUserStates.get(chatId);
        state.hideNavBar = (data === 'apk_navbar_yes');

        if (state.appType === 'temp') {
          // التطبيق المؤقت: القرار بخصوص google-services/keystore
          // محسوم مسبقاً (إما أصول موحّدة جاهزة، أو سيولَّد keystore
          // تلقائي أول مرة) — نتخطى السؤال مباشرة للأذونات.
          state.permIndex = 0;
          await apkEdit(chatId, msgId, '✅ تمام، جارٍ الانتقال للأذونات...');
          await apkAskNextPermission(chatId);
          return;
        }

        await apkEdit(chatId, msgId,
          '🔑 <b>هل لديك ملف google-services.json وملف keystore؟</b>',
          apkYesNoKeyboard('apk_gfiles_yes', 'apk_gfiles_no')
        );
        return;
      }

      // ── أسئلة الأذونات (نعم/لا لكل إذن) ──
      if (data.startsWith('apk_perm_yes_') || data.startsWith('apk_perm_no_')) {
        const state = apkUserStates.get(chatId);
        if (!state) return;
        const isYes = data.startsWith('apk_perm_yes_');
        const key = data.replace(isYes ? 'apk_perm_yes_' : 'apk_perm_no_', '');
        if (isYes) {
          state.permissions = state.permissions || [];
          state.permissions.push(key);
        }
        state.permIndex = (state.permIndex || 0) + 1;
        await apkAskNextPermission(chatId, msgId);
        return;
      }

      return;
    }

    // ════════════════════════════════════════════════════════════════
    // معالجة الرسائل العادية (نص أو ملف)
    // ════════════════════════════════════════════════════════════════
    const msg = update.message;
    if (!msg) return;
    const chatId = msg.chat.id;
    const text    = msg.text || '';
    const state   = apkUserStates.get(chatId);

    // ── /start ──
    if (text === '/start') {
      apkUserStates.delete(chatId);
      await apkSend(chatId,
        '🔨 <b>بوت بناء تطبيقات APK</b>\n\nهل تود البناء من:',
        apkSourceKeyboard()
      );
      return;
    }

    // ── /save_temp_keystore: حفظ أصول التطبيق المؤقت الموحّدة ──
    if (text === '/save_temp_keystore') {
      apkUserStates.set(chatId, { step: 'tempsave_wait_keystore_file' });
      await apkSend(chatId,
        '🔑 <b>حفظ أصول التطبيق المؤقت الموحّدة</b>\n\n' +
        'أرسل الآن ملف <code>keystore</code> (<code>.keystore</code> أو <code>.jks</code>) الذي وصلك من أول بناء.'
      );
      return;
    }

    // ── سلسلة حفظ التطبيق المؤقت: ملف keystore ──
    if (state && state.step === 'tempsave_wait_keystore_file') {
      const doc = msg.document;
      const fname = (doc && doc.file_name || '').toLowerCase();
      if (!doc || !(fname.endsWith('.keystore') || fname.endsWith('.jks'))) {
        await apkSend(chatId, '⚠️ أرسل ملف keystore بصيغة .keystore أو .jks');
        return;
      }
      try {
        await apkSend(chatId, '⏳ جارٍ رفع الملف لـ GitHub...');
        const { buffer } = await apkFetchTelegramFile(doc.file_id);
        state.tempKeystoreUrl = await apkUploadFileToGithub(buffer, doc.file_name);
        state.step = 'tempsave_wait_keystore_password';
        await apkSend(chatId, '🔒 أرسل <b>كلمة مرور</b> الـ keystore.');
      } catch (e) {
        console.error('[APKBuilder] خطأ رفع keystore المؤقت:', e.message);
        await apkSend(chatId, `❌ خطأ: ${e.message}`);
      }
      return;
    }

    // ── سلسلة حفظ التطبيق المؤقت: كلمة المرور ──
    if (state && state.step === 'tempsave_wait_keystore_password') {
      if (!text.trim()) { await apkSend(chatId, '⚠️ أرسل كلمة المرور كنص.'); return; }
      state.tempKeystorePassword = text.trim();
      state.step = 'tempsave_wait_keystore_alias';
      await apkSend(chatId, '🏷️ أرسل <b>alias</b> الخاص بمفتاح الـ keystore.');
      return;
    }

    // ── سلسلة حفظ التطبيق المؤقت: alias ──
    if (state && state.step === 'tempsave_wait_keystore_alias') {
      if (!text.trim()) { await apkSend(chatId, '⚠️ أرسل الـ alias كنص.'); return; }
      state.tempKeystoreAlias = text.trim();
      state.step = 'tempsave_wait_google_services';
      await apkSend(chatId, '📤 أرسل الآن ملف <code>google-services.json</code>.');
      return;
    }

    // ── سلسلة حفظ التطبيق المؤقت: google-services.json ──
    if (state && state.step === 'tempsave_wait_google_services') {
      const doc = msg.document;
      if (!doc || !(doc.file_name || '').toLowerCase().endsWith('.json')) {
        await apkSend(chatId, '⚠️ أرسل ملف google-services.json بصيغة .json');
        return;
      }
      try {
        await apkSend(chatId, '⏳ جارٍ رفع الملف لـ GitHub...');
        const { buffer } = await apkFetchTelegramFile(doc.file_id);
        const gsUrl = await apkUploadFileToGithub(buffer, 'google-services.json');

        await apkSend(chatId,
          `✅ <b>تم رفع كل الملفات بنجاح!</b>\n\n` +
          `الصق هذه القيم الأربع كمتغيرات ثابتة في <code>server.js</code> بدلاً من القيم الفارغة الحالية:\n\n` +
          `<code>const APK_TEMP_GOOGLE_SERVICES_URL = "${gsUrl}";\n` +
          `const APK_TEMP_KEYSTORE_URL        = "${state.tempKeystoreUrl}";\n` +
          `const APK_TEMP_KEYSTORE_PASSWORD   = "${state.tempKeystorePassword}";\n` +
          `const APK_TEMP_KEYSTORE_ALIAS      = "${state.tempKeystoreAlias}";</code>\n\n` +
          `بعد حفظ التعديل وإعادة تشغيل السيرفر، كل تطبيق مؤقت قادم سيستخدم هذه القيم تلقائياً بدون أي أسئلة إضافية.`
        );
        apkUserStates.delete(chatId);
      } catch (e) {
        console.error('[APKBuilder] خطأ رفع google-services المؤقت:', e.message);
        await apkSend(chatId, `❌ خطأ: ${e.message}`);
      }
      return;
    }

    // ── انتظار ملف index.html ──
    if (state && state.step === 'wait_html') {
      const doc = msg.document;
      if (!doc || !(doc.file_name || '').toLowerCase().endsWith('.html')) {
        await apkSend(chatId, '⚠️ أرسل ملف بصيغة <b>.html</b> فقط.');
        return;
      }
      try {
        await apkSend(chatId, '⏳ جارٍ رفع الملف...');
        const { buffer } = await apkFetchTelegramFile(doc.file_id);
        const htmlContent = buffer.toString('utf-8');
        const label = (doc.file_name || 'index').replace(/\.html$/i, '');
        const { slug, link, expiresAt } = await thCreateSiteFromHtml(htmlContent, chatId, label);
        const expDate = new Date(expiresAt).toLocaleDateString('ar-SY', { year: 'numeric', month: 'long', day: 'numeric' });

        await apkSend(chatId,
          `✅ <b>تم رفع الموقع بنجاح!</b>\n\n` +
          `🔗 <b>الرابط:</b>\n<code>${link}</code>\n\n` +
          `📅 ينتهي: ${expDate}`
        );

        state.sourceUrl = link;
        state.step = null;
        await apkSend(chatId,
          '📲 <b>هل تود إنشاء ملف APK WebView لهذا الرابط؟</b>',
          apkYesNoKeyboard('apk_build_yes', 'apk_build_no')
        );
      } catch (e) {
        console.error('[APKBuilder] خطأ HTML:', e.message);
        await apkSend(chatId, `❌ خطأ: ${e.message}`);
      }
      return;
    }

    // ── انتظار ملف index.html المحدّث (تحديث موقع موجود) ──
    if (state && state.step === 'wait_update_html_file') {
      const doc = msg.document;
      if (!doc || !(doc.file_name || '').toLowerCase().endsWith('.html')) {
        await apkSend(chatId, '⚠️ أرسل ملف بصيغة <b>.html</b> فقط.');
        return;
      }
      const slug = state.updateSlug;
      const site = tempHostSites.get(slug);
      if (!site) {
        await apkSend(chatId, '⚠️ هذا الموقع لم يعد موجوداً، أرسل /start من جديد.');
        apkUserStates.delete(chatId);
        return;
      }
      try {
        await apkSend(chatId, '⏳ جارٍ تحديث الموقع...');
        const { buffer } = await apkFetchTelegramFile(doc.file_id);
        const htmlContent = buffer.toString('utf-8');

        // تحديث المحتوى فقط — مع الإبقاء على createdAt/expiresAt الأصليين
        site.html = htmlContent;
        tempHostSites.set(slug, site);
        await thSaveToFirestore(slug, site);

        const expDate = new Date(site.expiresAt).toLocaleDateString('ar-SY', { year: 'numeric', month: 'long', day: 'numeric' });
        await apkSend(chatId,
          `✅ <b>تم تحديث الموقع بنجاح!</b>\n\n` +
          `🔗 <b>الرابط (بدون تغيير):</b>\n<code>${TEMPHOST_BASE_URL}/p/${slug}</code>\n\n` +
          `📅 ينتهي كما هو: ${expDate}`
        );
      } catch (e) {
        console.error('[APKBuilder] خطأ تحديث html:', e.message);
        await apkSend(chatId, `❌ خطأ: ${e.message}`);
      }
      apkUserStates.delete(chatId);
      return;
    }

    // ── انتظار رابط جاهز ──
    if (state && state.step === 'wait_ready_url') {
      const url = text.trim();
      if (!url.startsWith('http')) {
        await apkSend(chatId, '⚠️ الرابط غير صحيح. أرسل رابطاً يبدأ بـ https://');
        return;
      }
      state.sourceUrl = url;
      state.step = null;
      await apkSend(chatId,
        '📲 <b>هل تود إنشاء ملف APK WebView لهذا الرابط؟</b>',
        apkYesNoKeyboard('apk_build_yes', 'apk_build_no')
      );
      return;
    }

    // ── اسم التطبيق ──
    if (state && state.step === 'wait_app_name') {
      if (!text.trim()) { await apkSend(chatId, '⚠️ أرسل اسم التطبيق كنص.'); return; }
      state.appName = text.trim();
      if (state.appType === 'temp') {
        // الحزمة ثابتة مسبقاً للتطبيق المؤقت — نتخطى السؤال عنها
        state.step = 'wait_app_version';
        await apkSend(chatId, '🔢 <b>ما هو إصدار التطبيق؟</b>\n<i>مثال: 1.0.0</i>');
      } else {
        state.step = 'wait_app_package';
        await apkSend(chatId, '📦 <b>ما هي حزمة التطبيق (Package ID)؟</b>\n<i>مثال: com.example.app</i>');
      }
      return;
    }

    // ── حزمة التطبيق ──
    if (state && state.step === 'wait_app_package') {
      const pkg = text.trim();
      if (!/^[a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+$/.test(pkg)) {
        await apkSend(chatId, '⚠️ صيغة الحزمة غير صحيحة. مثال صحيح: com.example.app');
        return;
      }
      state.appPackage = pkg;
      state.step = 'wait_app_version';
      await apkSend(chatId, '🔢 <b>ما هو إصدار التطبيق؟</b>\n<i>مثال: 1.0.0</i>');
      return;
    }

    // ── إصدار التطبيق ──
    if (state && state.step === 'wait_app_version') {
      if (!text.trim()) { await apkSend(chatId, '⚠️ أرسل رقم الإصدار كنص، مثال: 1.0.0'); return; }
      state.appVersion = text.trim();
      state.step = 'wait_icon_url';
      await apkSend(chatId, '🖼️ <b>أرسل ملف أيقونة التطبيق بصيغة .zip</b>\n<i>يجب أن يكون ناتج IconKitchen ويحتوي مجلد android/res</i>');
      return;
    }

    // ── ملف zip الأيقونة (مخرجات IconKitchen) ──
    if (state && state.step === 'wait_icon_url') {
      const doc = msg.document;
      if (!doc || !(doc.file_name || '').toLowerCase().endsWith('.zip')) {
        await apkSend(chatId, '⚠️ أرسل ملف الأيقونة بصيغة .zip (مخرجات IconKitchen التي تحتوي مجلد android/res).');
        return;
      }
      try {
        await apkSend(chatId, '⏳ جارٍ رفع ملف الأيقونة...');
        const { buffer } = await apkFetchTelegramFile(doc.file_id);
        state.iconZipUrl = await apkUploadFileToGithub(buffer, doc.file_name);
        state.step = null;
        await apkSend(chatId,
          '👁️ <b>هل تريد إخفاء شريط الحالة (Status Bar)؟</b>',
          apkYesNoKeyboard('apk_statusbar_yes', 'apk_statusbar_no')
        );
      } catch (e) {
        console.error('[APKBuilder] خطأ رفع أيقونة zip:', e.message);
        await apkSend(chatId, `❌ خطأ: ${e.message}`);
      }
      return;
    }

    // ── ملف google-services.json ──
    if (state && state.step === 'wait_google_services_file') {
      const doc = msg.document;
      if (!doc || !(doc.file_name || '').toLowerCase().endsWith('.json')) {
        await apkSend(chatId, '⚠️ أرسل ملف <code>google-services.json</code> بصيغة .json');
        return;
      }
      try {
        await apkSend(chatId, '⏳ جارٍ رفع الملف...');
        const { buffer } = await apkFetchTelegramFile(doc.file_id);
        state.googleServicesUrl = await apkUploadFileToGithub(buffer, 'google-services.json');
        state.step = 'wait_keystore_file';
        await apkSend(chatId, '🔑 أرسل ملف <b>keystore</b> الآن (<code>.keystore</code> أو <code>.jks</code>).');
      } catch (e) {
        console.error('[APKBuilder] خطأ google-services:', e.message);
        await apkSend(chatId, `❌ خطأ: ${e.message}`);
      }
      return;
    }

    // ── ملف keystore ──
    if (state && state.step === 'wait_keystore_file') {
      const doc = msg.document;
      const fname = (doc && doc.file_name || '').toLowerCase();
      if (!doc || !(fname.endsWith('.keystore') || fname.endsWith('.jks'))) {
        await apkSend(chatId, '⚠️ أرسل ملف keystore بصيغة .keystore أو .jks');
        return;
      }
      try {
        await apkSend(chatId, '⏳ جارٍ رفع الملف...');
        const { buffer } = await apkFetchTelegramFile(doc.file_id);
        state.keystoreUrl = await apkUploadFileToGithub(buffer, doc.file_name);
        state.step = 'wait_keystore_password';
        await apkSend(chatId, '🔒 ما هي <b>كلمة مرور</b> الـ keystore؟');
      } catch (e) {
        console.error('[APKBuilder] خطأ keystore:', e.message);
        await apkSend(chatId, `❌ خطأ: ${e.message}`);
      }
      return;
    }

    // ── كلمة مرور keystore ──
    if (state && state.step === 'wait_keystore_password') {
      if (!text.trim()) { await apkSend(chatId, '⚠️ أرسل كلمة المرور كنص.'); return; }
      state.keystorePassword = text.trim();
      state.step = 'wait_keystore_alias';
      await apkSend(chatId, '🏷️ ما هو <b>alias</b> الخاص بمفتاح الـ keystore؟');
      return;
    }

    // ── alias الـ keystore ──
    if (state && state.step === 'wait_keystore_alias') {
      if (!text.trim()) { await apkSend(chatId, '⚠️ أرسل الـ alias كنص.'); return; }
      state.keystoreAlias = text.trim();
      state.step = null;
      state.permIndex = 0;
      await apkAskNextPermission(chatId);
      return;
    }

    // ── رسالة خارج سياق ──
    if (!state) {
      await apkSend(chatId, '👋 أهلاً! اضغط /start للبدء.');
    }

  } catch (e) {
    console.error('[APKBuilder] ❌ خطأ:', e.message);
  }
});

// ─── تسجيل webhook ───────────────────────────────────────────────────
async function apkSetupWebhook() {
  try {
    const url  = `https://mahmoud08808665888888m-my-bot.hf.space/apkbuilder-webhook`;
    const r    = await fetch(`${APK_BOT_API}/setWebhook?url=${encodeURIComponent(url)}`);
    const data = await r.json();
    if (data.ok) console.log('[APKBuilder] ✅ Webhook مسجّل:', url);
    else console.error('[APKBuilder] ❌ Webhook:', data.description);
    await reqSetBotStartCommand(APK_BOT_API);
  } catch (e) { console.error('[APKBuilder] ❌ Webhook error:', e.message); }
}

// ══════════════════════════════════════════════════════════════════════

// ══════════════════════════════════════════════════════════════════════

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
  console.log('Caro Proxy running on port ' + PORT);

  // Web Push polling مُعطّل حالياً (طلب إيقاف بتاريخ 2026-07-04 بسبب
  // أخطاء متكررة "Missing or insufficient permissions" على Firestore).
  // لإعادة التفعيل: احذف التعليق عن السطر التالي.
  // startWebPushPolling();

  // تهيئة بوت APK Builder
  apkSetupWebhook().catch(e => console.error('[APKBuilder] خطأ webhook:', e.message));

  // تسجيل webhook تلقائي لبوت خدمات كارو (نفس منطق /register-services-webhook)
  // بيضمن إنه البوت يسجل نفسه تلقائياً كل مرة يشتغل فيها السيرفر من جديد
  (async () => {
    try {
      const servicesWebhookUrl = 'https://mahmoud08808665888888m-my-bot.hf.space/services-bot-webhook';
      const r = await fetch(SERVICES_BOT_API + '/setWebhook', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url: servicesWebhookUrl })
      });
      const data = await r.json();
      if (data.ok) console.log('[ServicesBot] ✅ Webhook مسجّل تلقائياً:', servicesWebhookUrl);
      else console.error('[ServicesBot] ❌ فشل تسجيل Webhook:', data.description);
      await reqSetBotStartCommand(SERVICES_BOT_API);
    } catch (e) {
      console.error('[ServicesBot] ❌ خطأ في تسجيل Webhook التلقائي:', e.message);
    }
  })();

  // استرجاع المواقع المرفوعة سابقاً (تُستخدم من بوت الإنشاء)
  thRestoreFromFirestore().catch(e => console.error('[TempHost] خطأ في الاسترجاع:', e.message));

  // تهيئة بوت الطلبات وتسجيل webhooks
  reqInitData().then(() => reqSetupWebhooks()).catch(e => {
    console.error('[RequestBot] خطأ في التهيئة:', e.message);
    if (!requestAppData) requestAppData = { ...REQUEST_DEFAULT_DATA };
  });

  // ping تلقائي كل 25 دقيقة لمنع النوم
  setInterval(async () => {
    try {
      await fetch('https://mahmoud08808665888888m-my-bot.hf.space/');
      console.log('[ping] Server kept alive');
    } catch(e) {
      console.error('[ping] Failed:', e.message);
    }
  }, 25 * 60 * 1000);
});
