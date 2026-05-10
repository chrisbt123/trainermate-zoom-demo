(function(){
  function setText(id, value){
    var el = document.getElementById(id);
    if(el) el.textContent = value || '';
  }

  function applyCertificateStatus(data){
    var cert = data && data.certificate_scan ? data.certificate_scan : {};
    var latest = cert.latest || {};
    var running = !!(data && data.certificate_running);
    var progress = document.getElementById('tmCertificateProgress');
    var workspace = document.getElementById('tmCertificateWorkspace');
    var state = document.getElementById('tmCertificateProgressState');
    var status = String(latest.status || '').toLowerCase();
    var title = latest.message || (running ? 'Checking certificates' : 'Certificate check complete');
    var detail = latest.detail || (running ? 'Reading provider certificate lists.' : 'FOBS certificate overview is up to date.');
    setText('tmCertificateProgressTitle', title);
    setText('tmCertificateProgressDetail', detail);
    setText('tmCertificateBusyTitle', title);
    setText('tmCertificateBusyDetail', detail);
    if(progress){
      progress.classList.remove('idle','running','error');
      progress.classList.add(running ? 'running' : (status === 'error' ? 'error' : 'idle'));
    }
    if(state){
      state.textContent = running ? 'working' : (latest.status || 'idle');
      state.className = 'status-tag ' + (running ? 'due' : (status === 'error' ? 'bad' : 'neutral'));
    }
    if(workspace) workspace.classList.toggle('certificate-busy', running);
    var controls = document.querySelectorAll('.certificate-job-control');
    for(var i = 0; i < controls.length; i++) controls[i].disabled = running;
  }

  function pollCertificateStatus(){
    if(!document.getElementById('tmCertificateWorkspace')) return;
    fetch('/live-status?_=' + Date.now(), {cache:'no-store'})
      .then(function(response){ if(!response.ok) throw new Error('HTTP ' + response.status); return response.json(); })
      .then(applyCertificateStatus)
      .catch(function(){});
  }

  pollCertificateStatus();
  window.setInterval(pollCertificateStatus, 1000);
})();

(function(){
  if(!document.getElementById('tmCertificateWorkspace')) return;
  var key = 'tmCertificateAutoReloadSawRunning';
  var reloadKey = 'tmCertificateAutoReloadedAt';
  var initialUpdate = null;
  var initialStatus = null;
  var pageLoadedAt = Date.now();

  function finishStatus(status){
    status = String(status || '').toLowerCase();
    return ['complete','error','cancelled','idle'].indexOf(status) >= 0;
  }

  function reloadCertificates(){
    var last = Number(sessionStorage.getItem(reloadKey) || '0');
    if(Date.now() - last <= 2500) return;
    sessionStorage.setItem(reloadKey, String(Date.now()));
    var target = new URL(window.location.href);
    target.searchParams.set('section', 'files');
    target.searchParams.set('certificates_refreshed', String(Date.now()));
    window.location.href = target.toString();
  }

  async function poll(){
    try{
      var r = await fetch('/live-status?_=' + Date.now(), {cache:'no-store'});
      if(!r.ok) return;
      var data = await r.json();
      var cert = data && data.certificate_scan ? data.certificate_scan : {};
      var latest = cert.latest || {};
      var running = !!data.certificate_running;
      var updated = String(latest.updated_at || '');
      var status = String(latest.status || '').toLowerCase();

      if(initialUpdate === null){
        initialUpdate = updated;
        initialStatus = status;
      }

      if(running){
        sessionStorage.setItem(key, '1');
        return;
      }

      // Reload once a visible/background certificate scan finishes so the
      // provider certificate table reflects the freshly saved FOBS cache.
      if(sessionStorage.getItem(key) === '1' && finishStatus(status)){
        sessionStorage.removeItem(key);
        reloadCertificates();
        return;
      }

      if(updated && updated !== initialUpdate && finishStatus(status) && Date.now() - pageLoadedAt > 700){
        reloadCertificates();
      }
    }catch(e){}
  }

  window.setInterval(poll, 1200);
  window.setTimeout(poll, 300);
})();
