/* ==========================================================================
   app2-voice.js — 語音輸入 subsystem (路名 / 學區 input)
   --------------------------------------------------------------------------
   從 app2.js 拆出來（Sprint 1.B 第一階段）。自包含：只用 window.SpeechRecognition
   + DOM API，沒有 closure deps，搬位置後完全等價。

   隔離原則同 map_mode.js / cadastral_search.js：
     - 必須在 app2.js 之後 load（這樣 window.v2 已存在）
     - 透過 window.v2._startVoice / window.v2.startVoiceRoad / startVoiceSchool
       對外暴露；app2.js 不再定義這 3 個 function
   ========================================================================== */
(function () {
  'use strict';

  window.v2 = window.v2 || {};

  const $ = (sel) => document.querySelector(sel);

  function _showVoiceStatus(text, kind) {
    let host = $('#v2-voice-banner');
    if (!host) {
      host = document.createElement('div');
      host.id = 'v2-voice-banner';
      document.body.appendChild(host);
    }
    host.className = 'v2-voice-banner' + (kind ? ' v2-voice-banner--' + kind : '');
    host.textContent = text;
    host.style.display = 'block';
    if (kind === 'error' || kind === 'success') {
      setTimeout(() => { if (host.textContent === text) host.style.display = 'none'; }, 3000);
    }
  }

  const _VOICE_ERR_TXT = {
    'no-speech': '沒偵測到語音 — 請靠近麥克風再試一次',
    'audio-capture': '麥克風無法使用 — 請檢查裝置麥克風',
    'not-allowed': '瀏覽器拒絕麥克風權限 — 請在設定→Safari/Chrome 開啟麥克風',
    'service-not-allowed': '系統拒絕語音服務 — 改用其他瀏覽器試試',
    'network': '語音辨識需要網路連線',
    'language-not-supported': '不支援中文辨識',
    'aborted': '語音辨識被取消',
  };

  function _startVoice(inputId, btnId) {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    const inp = $('#' + inputId);
    const btn = $('#' + btnId);
    if (!SR) {
      _showVoiceStatus('此瀏覽器不支援語音輸入。Chrome 或 iOS Safari 較新版本才支援', 'error');
      return;
    }
    if (!inp) return;
    if (!window.isSecureContext) {
      _showVoiceStatus('語音輸入只能在 HTTPS 環境用', 'error');
      return;
    }
    const rec = new SR();
    rec.lang = 'zh-TW';
    rec.interimResults = true;
    rec.maxAlternatives = 1;
    rec.continuous = false;
    if (btn) btn.classList.add('v2-road-mic--active');
    let gotAnyResult = false;
    rec.onaudiostart = () => _showVoiceStatus('🎤 麥克風已開啟，請說話…', 'listening');
    rec.onspeechstart = () => _showVoiceStatus('🎙 偵測到聲音，繼續說…', 'listening');
    rec.onresult = (e) => {
      let interim = '', final = '';
      for (let i = e.resultIndex; i < e.results.length; i++) {
        const tr = e.results[i][0].transcript || '';
        if (e.results[i].isFinal) final += tr;
        else interim += tr;
      }
      if (interim) {
        gotAnyResult = true;
        _showVoiceStatus('辨識中：' + interim, 'listening');
      }
      if (final) {
        gotAnyResult = true;
        const text = final.trim().replace(/\s+/g, '').replace(/。$/, '');
        inp.value = text;
        inp.dispatchEvent(new Event('input', { bubbles: true }));
        _showVoiceStatus('✓ 已輸入：' + text, 'success');
      }
    };
    rec.onnomatch = () => _showVoiceStatus('沒辨識出內容，再試一次', 'error');
    rec.onerror = (ev) => {
      _showVoiceStatus(_VOICE_ERR_TXT[ev.error] || ('語音辨識錯誤：' + ev.error), 'error');
      if (btn) btn.classList.remove('v2-road-mic--active');
    };
    rec.onend = () => {
      if (btn) btn.classList.remove('v2-road-mic--active');
      if (!gotAnyResult) {
        const host = $('#v2-voice-banner');
        if (!host || host.className.indexOf('--error') < 0) {
          _showVoiceStatus('未偵測到語音，請靠近麥克風再試一次', 'error');
        }
      }
    };
    try {
      rec.start();
    } catch (e) {
      _showVoiceStatus('語音啟動失敗：' + (e.message || e), 'error');
      if (btn) btn.classList.remove('v2-road-mic--active');
    }
  }

  window.v2.startVoiceRoad   = function () { _startVoice('v2-road',   'v2-road-mic'); };
  window.v2.startVoiceSchool = function () { _startVoice('v2-school', 'v2-school-mic'); };
})();
