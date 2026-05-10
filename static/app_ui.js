(function(){
  var config = window.TRAINERMATE_UI_CONFIG || {};
  var csrfToken = config.csrfToken || '';
  var presets = config.providerPresets || {};

  function tmHideDismissedContainer(source){
    var root = source && source.closest && source.closest('.alert-item,.certificate-attention-row,.tm-activity-bubble,.card.unread,.card,.provider-card');
    if(root){
      root.style.display = 'none';
      var attention = root.closest && root.closest('.certificate-attention');
      if(attention && !attention.querySelector('.certificate-attention-row:not([style*="display: none"])')){
        attention.style.display = 'none';
        var workspace = document.getElementById('tmCertificateWorkspace');
        if(workspace) workspace.classList.remove('certificate-has-attention');
      }
    }
  }

  function tmOpenModal(title, message){
    var modal = document.getElementById('tmInfoModal');
    if(!modal) return;
    var titleEl = document.getElementById('tmInfoModalTitle');
    var msgEl = document.getElementById('tmInfoModalMessage');
    if(titleEl) titleEl.textContent = title || 'TrainerMate';
    if(msgEl) msgEl.textContent = message || '';
    modal.hidden = false;
    document.body.classList.add('tm-modal-open');
  }

  function tmCloseModal(){
    var modal = document.getElementById('tmInfoModal');
    if(!modal) return;
    modal.hidden = true;
    document.querySelectorAll('.tm-top-flash').forEach(function(el){ el.hidden = true; });
    document.body.classList.remove('tm-modal-open');
  }

  window.tmHideDismissedContainer = tmHideDismissedContainer;
  window.tmOpenModal = tmOpenModal;
  window.tmCloseModal = tmCloseModal;

  document.addEventListener('click', function(e){
    var closeTrigger = e.target && e.target.closest && e.target.closest('[data-tm-modal-close],#tmInfoModalClose,#tmInfoModalX,.tm-modal-close-x');
    if(closeTrigger || (e.target && e.target.id === 'tmInfoModal')){
      e.preventDefault();
      e.stopPropagation();
      tmCloseModal();
      return false;
    }
    var calendarClose = e.target && e.target.closest && e.target.closest('[data-calendar-modal-close],#tmModalClose');
    if(calendarClose){
      var modal = document.getElementById('tmCourseModal');
      if(modal){
        e.preventDefault();
        e.stopPropagation();
        modal.classList.remove('open');
        modal.setAttribute('aria-hidden', 'true');
        return false;
      }
    }
  }, true);

  document.addEventListener('submit', function(e){
    var form = e.target;
    if(!form || !form.matches || !form.matches('form')) return;
    var action = (form.getAttribute('action') || '').toLowerCase();
    var isDismiss = action.indexOf('/dismiss') >= 0 || action.indexOf('dismiss-missing') >= 0 || form.classList.contains('tm-instant-dismiss-form');
    if(!isDismiss) return;
    if(!form.querySelector('input[name="_csrf_token"]')){
      var token = document.createElement('input');
      token.type = 'hidden';
      token.name = '_csrf_token';
      token.value = csrfToken;
      form.appendChild(token);
    }
    e.preventDefault();
    e.stopPropagation();
    tmHideDismissedContainer(form);
    fetch(form.action, {method: form.method || 'POST', body: new FormData(form), credentials: 'same-origin', headers: {'X-Requested-With': 'fetch'}})
      .catch(function(){ form.submit(); });
    return false;
  }, true);

  document.addEventListener('click', function(e){
    var trigger = e.target.closest && e.target.closest('.tm-paid-modal-trigger');
    if(trigger){ e.preventDefault(); tmOpenModal(trigger.dataset.title, trigger.dataset.message); return; }
    var closeTrigger = e.target.closest && e.target.closest('[data-tm-modal-close],#tmInfoModalClose,#tmInfoModalX');
    if(closeTrigger || e.target.id === 'tmInfoModal'){
      e.preventDefault();
      tmCloseModal();
    }
  });

  document.addEventListener('keydown', function(e){ if(e.key === 'Escape') tmCloseModal(); });
  window.addEventListener('load', function(){
    var modal = document.getElementById('tmInfoModal');
    if(modal && !modal.hidden) document.body.classList.add('tm-modal-open');
  });

  window.tmProviderPresetChanged = function(selectEl) {
    var s = selectEl || document.getElementById('provider_preset');
    var n = document.getElementById('provider_name');
    var u = document.getElementById('login_url');
    var option = s && s.options ? s.options[s.selectedIndex] : null;
    var isManual = !!s && s.value === 'manual';
    var name = option ? (option.getAttribute('data-provider-name') || '') : '';
    var url = option ? (option.getAttribute('data-login-url') || '') : '';
    if (n) {
      n.value = isManual ? '' : name;
      n.readOnly = !isManual;
      n.required = isManual;
      n.placeholder = isManual ? 'Provider name' : name;
    }
    if (u) {
      u.value = isManual ? '' : url;
      u.readOnly = !isManual && !!url;
      u.required = isManual;
      u.placeholder = isManual ? 'https://.../Account/Login' : (url || 'FOBS details not confirmed yet');
    }
    document.querySelectorAll('.manual-provider-field').forEach(function(field){
      field.style.display = isManual ? 'grid' : 'none';
    });
  };

  (function(){
    var screen = document.getElementById('tmStartupScreen');
    if (!screen) return;
    var title = document.getElementById('tmStartupTitle');
    var subtitle = document.getElementById('tmStartupSubtitle');
    var stepsEl = document.getElementById('tmStartupSteps');
    var skip = document.getElementById('tmStartupSkip');
    var skippedKey = 'tmStartupScreenSkippedUntil';
    var skipMs = 12 * 60 * 60 * 1000;
    var hidden = false;
    var startedAt = Date.now();
    var intervalId = null;
    var allowOverlay = config.showStartupOverlay === true;

    function hide(){
      if (hidden) return;
      hidden = true;
      screen.classList.remove('show');
      if (intervalId) window.clearInterval(intervalId);
    }

    function shouldStaySkipped(){
      try {
        var until = Number(localStorage.getItem(skippedKey) || '0');
        return until && Date.now() < until;
      } catch (_) {
        return false;
      }
    }

    function rememberSkip(){
      try { localStorage.setItem(skippedKey, String(Date.now() + skipMs)); } catch (_) {}
    }

    function showIfUseful(data){
      if (!allowOverlay || hidden) return;
      if (!data || !data.running) return;
      screen.classList.add('show');
    }

    function iconFor(status){
      status = (status || '').toLowerCase();
      if (status === 'complete') return 'OK';
      if (status === 'skipped') return '-';
      if (status === 'warning' || status === 'error') return '!';
      return '...';
    }

    function render(data){
      if (!stepsEl) return;
      if (title) title.textContent = data.message || 'Getting TrainerMate ready';
      if (subtitle) subtitle.textContent = data.running ? 'TrainerMate is getting today ready.' : 'Ready when you are.';
      stepsEl.innerHTML = '';
      (data.steps || []).forEach(function(step){
        var status = (step.status || 'waiting').toLowerCase();
        var row = document.createElement('div');
        row.className = 'startup-step state-' + status;
        var icon = document.createElement('span');
        icon.className = 'startup-step-icon';
        icon.textContent = iconFor(status);
        var main = document.createElement('span');
        main.className = 'startup-step-main';
        var label = document.createElement('span');
        label.className = 'startup-step-label';
        label.textContent = step.label || '';
        var detail = document.createElement('span');
        detail.className = 'startup-step-detail';
        detail.textContent = step.detail || '';
        var state = document.createElement('span');
        state.className = 'startup-step-state';
        state.textContent = status === 'complete' ? 'done' : status;
        main.appendChild(label);
        main.appendChild(detail);
        row.appendChild(icon);
        row.appendChild(main);
        row.appendChild(state);
        stepsEl.appendChild(row);
      });
    }

    async function pollStartup(){
      if (hidden) return;
      try {
        var r = await fetch('/startup-status?_=' + Date.now(), {cache: 'no-store'});
        if (!r.ok) throw new Error('HTTP ' + r.status);
        var data = await r.json();
        render(data);
        showIfUseful(data);
        if (data.done || (!data.running && Date.now() - startedAt > 1600)) {
          window.setTimeout(hide, 500);
        }
      } catch (_) {
        if (Date.now() - startedAt > 3500) hide();
      }
      if (Date.now() - startedAt > 14000) hide();
    }

  if (skip) {
      skip.addEventListener('click', function(){
        rememberSkip();
        hide();
      });
    }

    if (shouldStaySkipped()) return;
    pollStartup();
    intervalId = window.setInterval(pollStartup, 850);
  })();

  (function(){
    if (!config.certificateBusy) return;
    var reloaded = false;
    async function waitForCertificateMirror(){
      if (reloaded) return;
      try {
        var r = await fetch('/startup-status?_=' + Date.now(), {cache: 'no-store'});
        if (r.ok) {
          var data = await r.json();
          var providerStep = (data.steps || []).find(function(step){ return step && step.key === 'providers'; }) || {};
          var state = String(providerStep.status || '').toLowerCase();
          if (data.done === true || state === 'complete' || state === 'skipped' || state === 'error' || state === 'warning' || state === 'cancelled') {
            reloaded = true;
            window.location.reload();
            return;
          }
        }
      } catch (_) {}
      try {
        var r2 = await fetch('/live-status?_=' + Date.now(), {cache: 'no-store'});
        if (!r2.ok) return;
        var data2 = await r2.json();
        var cert = data2.certificate_scan || {};
        var latest = cert.latest || {};
        var certRunning = !!data2.certificate_running;
        var status = String(latest.status || '').toLowerCase();
        if (!certRunning && (status === 'complete' || status === 'error' || status === 'cancelled' || status === 'idle' || status === 'skipped')) {
          reloaded = true;
          window.location.reload();
        }
      } catch (_) {}
    }
    window.setInterval(waitForCertificateMirror, 1000);
    window.setTimeout(waitForCertificateMirror, 400);
  })();

  var presetSelect = document.getElementById('provider_preset');
  var providerName = document.getElementById('provider_name');
  var loginUrl = document.getElementById('login_url');
  var providerColor = document.getElementById('provider_color');

  function syncManagedZoom(root) {
    var forms = root.querySelectorAll ? root.querySelectorAll('form') : [];
    forms.forEach(function(form){
      var managed = form.querySelector('.provider-managed-toggle');
      var overwrite = form.querySelector('.overwrite-toggle');
      if (!managed) return;
      var refresh = function(){
        if (overwrite && managed.checked) {
          overwrite.checked = true;
          overwrite.disabled = true;
          if(overwrite.closest('.checkbox')) overwrite.closest('.checkbox').classList.add('linked-setting');
        } else if (overwrite) {
          overwrite.disabled = false;
          if(overwrite.closest('.checkbox')) overwrite.closest('.checkbox').classList.remove('linked-setting');
        }
      };
      managed.addEventListener('change', refresh);
      refresh();
    });
  }

  window.syncManagedZoom = syncManagedZoom;

  if (presetSelect && providerName && loginUrl) {
    var applyProviderPreset = function(){
      var option = presetSelect.options[presetSelect.selectedIndex];
      var selected = presets[presetSelect.value] || {};
      var isManual = presetSelect.value === 'manual';
      var optionName = option && option.getAttribute('data-provider-name') ? option.getAttribute('data-provider-name') : '';
      var optionUrl = option && option.getAttribute('data-login-url') ? option.getAttribute('data-login-url') : '';
      var name = String(selected.name || optionName || '').trim();
      var url = String(selected.login_url || optionUrl || '').trim();
      if (!isManual) providerName.value = name;
      else if (!providerName.value) providerName.value = '';
      loginUrl.value = isManual ? loginUrl.value : url;
      providerName.readOnly = !isManual;
      loginUrl.readOnly = !isManual && !!url;
      providerName.placeholder = isManual ? 'Provider name' : name;
      loginUrl.placeholder = isManual ? 'https://.../Account/Login' : (url || 'FOBS details not confirmed yet');
      if (providerColor && typeof selected.color === 'string') providerColor.value = selected.color;
      var managed = document.querySelector('.provider-managed-toggle');
      if (managed && typeof selected.provider_manages_zoom !== 'undefined') managed.checked = !!selected.provider_manages_zoom;
      syncManagedZoom(document);
      window.tmProviderPresetChanged(presetSelect);
    };
    presetSelect.addEventListener('change', applyProviderPreset);
    presetSelect.addEventListener('change', function(){ window.tmProviderPresetChanged(presetSelect); });
    presetSelect.addEventListener('click', function(){ window.tmProviderPresetChanged(presetSelect); });
    applyProviderPreset();
    window.tmProviderPresetChanged(presetSelect);
  }
  syncManagedZoom(document);

  (function(){
    var rows = Array.from(document.querySelectorAll('[data-auto-dismiss-alert-id]'));
    if (!rows.length) return;
    var sent = false;
    var dismissSeenCertificateNotices = function(){
      if (sent) return;
      sent = true;
      var ids = Array.from(new Set(rows.flatMap(function(row){ return (row.getAttribute('data-auto-dismiss-alert-id') || '').split(','); }).map(function(id){ return id.trim(); }).filter(Boolean)));
      if (!ids.length) return;
      var form = new FormData();
      form.append('_csrf_token', csrfToken);
      ids.forEach(function(id){ form.append('alert_ids', id); });
      var url = config.autoDismissMissingCertificatePromptsUrl || '/certificates/missing-prompts/auto-dismiss';
      try {
        if (navigator.sendBeacon) {
          navigator.sendBeacon(url, form);
          return;
        }
      } catch (_) {}
      try {
        fetch(url, {method: 'POST', body: form, keepalive: true, credentials: 'same-origin'});
      } catch (_) {}
    };
    window.addEventListener('pagehide', dismissSeenCertificateNotices);
    document.addEventListener('visibilitychange', function(){ if (document.visibilityState === 'hidden') dismissSeenCertificateNotices(); });
  })();

  (function(){
    document.querySelectorAll('form').forEach(function(form){
      if (form.dataset.tmSubmitGuard === '1') return;
      form.dataset.tmSubmitGuard = '1';
      form.addEventListener('submit', function(event){
        if (event.defaultPrevented) return true;
        var method = (form.getAttribute('method') || 'get').toLowerCase();
        if (method === 'post' && !form.querySelector('input[name="_csrf_token"]')) {
          var token = document.createElement('input');
          token.type = 'hidden';
          token.name = '_csrf_token';
          token.value = csrfToken;
          form.appendChild(token);
        }
        if (form.dataset.tmSubmitting === '1') {
          event.preventDefault();
          return false;
        }
        form.dataset.tmSubmitting = '1';
        window.setTimeout(function(){
          form.querySelectorAll('button[type="submit"],input[type="submit"]').forEach(function(button){
            button.disabled = true;
            if (button.tagName.toLowerCase() === 'button' && !button.dataset.tmOriginalText) {
              button.dataset.tmOriginalText = button.textContent || '';
              if ((button.textContent || '').trim()) button.textContent = 'Working...';
            }
          });
        }, 0);
        return true;
      });
    });
  })();

  (function(){
    var debugDetailsEl = document.getElementById('debugDetails');
    var debugLogEl = document.getElementById('debugLog');
    var clearDebugBtn = document.getElementById('clearDebugBtn');
    var copyDebugBtn = document.getElementById('copyDebugBtn');
    var providerEl = document.getElementById('debugProvider');
    var courseEl = document.getElementById('debugCourse');
    var messageEl = document.getElementById('debugMessage');

    if (!debugLogEl) return;

    var userPinnedToBottom = true;
    var lastText = debugLogEl.textContent || '';
    var clearHoldUntil = 0;

    function isNearBottom() {
      return (debugLogEl.scrollHeight - debugLogEl.scrollTop - debugLogEl.clientHeight) < 24;
    }

    function forceBottom() {
      debugLogEl.scrollTop = debugLogEl.scrollHeight;
    }

    function setTerminalText(text) {
      text = text || '';
      if (text === lastText) return;
      lastText = text;
      debugLogEl.textContent = text;
      if (userPinnedToBottom) {
        forceBottom();
        requestAnimationFrame(forceBottom);
      }
    }

    debugLogEl.addEventListener('scroll', function(){
      userPinnedToBottom = isNearBottom();
      debugLogEl.classList.toggle('user-scrolled', !userPinnedToBottom);
    }, {passive: true});

    if (debugDetailsEl) {
      debugDetailsEl.addEventListener('toggle', function(){
        if (debugDetailsEl.open && userPinnedToBottom) requestAnimationFrame(forceBottom);
      });
    }

    async function refreshDebugState() {
      try {
        var r = await fetch('/debug-state?_=' + Date.now(), {cache: 'no-store'});
        if (!r.ok) return;
        var s = await r.json();
        if (providerEl) providerEl.textContent = s.current_provider || '-';
        if (courseEl) courseEl.textContent = s.current_course || '-';
        if (messageEl) messageEl.textContent = s.last_message || s.last_status || s.last_run_status || 'Idle';
      } catch (_) {}
    }

    async function refreshDebugLog() {
      if (Date.now() < clearHoldUntil) return;
      try {
        var wasPinned = isNearBottom();
        if (wasPinned) userPinnedToBottom = true;
        var r = await fetch('/debug-log?lines=180&_=' + Date.now(), {cache: 'no-store'});
        if (!r.ok) throw new Error('HTTP ' + r.status);
        var data = await r.json();
        var text = typeof data.text === 'string' ? data.text : ((data.lines || []).join('\n'));
        setTerminalText(text);
      } catch (err) {
        setTerminalText('Live log unavailable: ' + err.message);
      }
    }

    async function clearDebugLog() {
      clearHoldUntil = Date.now() + 1200;
      userPinnedToBottom = true;
      setTerminalText('');
      forceBottom();
      try {
        var r = await fetch('/debug-log/clear?_=' + Date.now(), {
          method: 'POST',
          cache: 'no-store',
          headers: {'Cache-Control': 'no-cache'}
        });
        if (!r.ok) throw new Error('HTTP ' + r.status);
        setTerminalText('');
        setTimeout(refreshDebugLog, 1300);
      } catch (err) {
        setTerminalText('Could not clear log: ' + err.message);
      }
    }

    function fallbackCopy(text) {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.left = '-9999px';
      ta.style.top = '0';
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      var ok = document.execCommand('copy');
      document.body.removeChild(ta);
      if (!ok) throw new Error('copy command failed');
    }

    async function copyLast30Lines() {
      var text = (debugLogEl.textContent || '').split('\n').slice(-30).join('\n');
      try {
        if (navigator.clipboard && window.isSecureContext) {
          await navigator.clipboard.writeText(text);
        } else {
          fallbackCopy(text);
        }
        copyDebugBtn.textContent = 'Copied';
      } catch (err) {
        try {
          fallbackCopy(text);
          copyDebugBtn.textContent = 'Copied';
        } catch (_) {
          copyDebugBtn.textContent = 'Copy failed';
        }
      }
      setTimeout(function(){ copyDebugBtn.textContent = 'Copy last 30 lines'; }, 1200);
    }

    if (clearDebugBtn) clearDebugBtn.onclick = clearDebugLog;
    if (copyDebugBtn) copyDebugBtn.onclick = copyLast30Lines;

    userPinnedToBottom = true;
    forceBottom();
    refreshDebugState();
    refreshDebugLog();
    setInterval(refreshDebugState, 1000);
    setInterval(refreshDebugLog, 750);
    window.addEventListener('load', function(){ requestAnimationFrame(forceBottom); });
  })();
})();
