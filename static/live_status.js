(function(){
  function setText(id, value){
    var el = document.getElementById(id);
    if(el) el.textContent = value || '-';
  }

  function makeRow(left, right){
    var div = document.createElement('div');
    div.className = 'live-status-row';
    var a = document.createElement('span');
    var b = document.createElement('span');
    a.textContent = left || '';
    b.textContent = right || '';
    div.appendChild(a);
    div.appendChild(b);
    return div;
  }

  var tmCertificateRefreshPending = sessionStorage.getItem('tmCertificateRefreshPending') === '1';
  var tmCertificateRefreshSawRunning = sessionStorage.getItem('tmCertificateRefreshSawRunning') === '1';
  var tmObservedCertificateRunning = sessionStorage.getItem('tmObservedCertificateRunning') === '1';
  var tmCertificateStayInPlace = sessionStorage.getItem('tmCertificateStayInPlace') === '1';
  var tmCertificateDeleteRow = null;
  var tmCertificateDeleteIds = [];
  function markCertificateRefreshStarted(){
    tmCertificateRefreshPending = true;
    tmCertificateRefreshSawRunning = false;
    tmCertificateStayInPlace = false;
    sessionStorage.setItem('tmCertificateRefreshPending', '1');
    sessionStorage.setItem('tmCertificateRefreshSawRunning', '0');
    sessionStorage.removeItem('tmCertificateStayInPlace');
  }
  function markCertificateInPlaceStarted(){
    tmCertificateRefreshPending = true;
    tmCertificateRefreshSawRunning = false;
    tmCertificateStayInPlace = true;
    sessionStorage.setItem('tmCertificateRefreshPending', '1');
    sessionStorage.setItem('tmCertificateRefreshSawRunning', '0');
    sessionStorage.setItem('tmCertificateStayInPlace', '1');
  }
  function markCertificateRefreshRunning(){
    tmCertificateRefreshSawRunning = true;
    tmObservedCertificateRunning = true;
    sessionStorage.setItem('tmCertificateRefreshSawRunning', '1');
    sessionStorage.setItem('tmObservedCertificateRunning', '1');
  }
  function clearCertificateRefreshMarker(){
    tmCertificateRefreshPending = false;
    tmCertificateRefreshSawRunning = false;
    tmObservedCertificateRunning = false;
    sessionStorage.removeItem('tmCertificateRefreshPending');
    sessionStorage.removeItem('tmCertificateRefreshSawRunning');
    sessionStorage.removeItem('tmObservedCertificateRunning');
    sessionStorage.removeItem('tmCertificateStayInPlace');
  }
  function reloadCertificatesView(){
    clearCertificateRefreshMarker();
    var target = new URL(window.location.href);
    target.searchParams.set('section', 'files');
    target.searchParams.set('certificates_refreshed', Date.now().toString());
    window.location.href = target.toString();
  }
  function refreshCertificateProviderCount(card){
    if(!card) return;
    var count = card.querySelectorAll('tr[data-provider-certificate-id]').length;
    var tag = card.querySelector('.provider-summary-right .status-tag');
    if(tag) tag.textContent = count + ' found';
    var tableWrap = card.querySelector('.tablewrap');
    if(count === 0 && tableWrap){
      tableWrap.remove();
      if(!card.querySelector('.empty')){
        var empty = document.createElement('div');
        empty.className = 'empty';
        var nameNode = card.querySelector('.provider-summary-main strong');
        empty.textContent = 'No FOBS certificates currently shown for ' + ((nameNode && nameNode.textContent) || 'this provider') + '.';
        card.appendChild(empty);
      }
    }
  }
  function removeCertificateRowsInPlace(ids, fallbackRow){
    var removedCards = [];
    (ids || []).forEach(function(id){
      if(!id) return;
      var row = document.querySelector('tr[data-provider-certificate-id="' + String(id).replace(/"/g, '\\"') + '"]');
      if(row){
        var card = row.closest('.cert-provider-card');
        row.remove();
        if(card && removedCards.indexOf(card) < 0) removedCards.push(card);
      }
    });
    if(fallbackRow && fallbackRow.isConnected){
      var fallbackCard = fallbackRow.closest('.cert-provider-card');
      fallbackRow.remove();
      if(fallbackCard && removedCards.indexOf(fallbackCard) < 0) removedCards.push(fallbackCard);
    }
    removedCards.forEach(refreshCertificateProviderCount);
  }
  function finishCertificateInPlace(latestStatus){
    var row = tmCertificateDeleteRow;
    clearCertificateRefreshMarker();
    if(row && (latestStatus || '').toLowerCase() === 'complete'){
      removeCertificateRowsInPlace(tmCertificateDeleteIds, row);
    } else if(row) {
      row.querySelectorAll('button,input,select,textarea').forEach(function(control){ control.disabled = false; });
      row.querySelectorAll('.provider-certificate-delete-form').forEach(function(form){ form.classList.remove('is-working'); form.dataset.tmSubmitting = '0'; });
      row.querySelectorAll('.btn.remove-fobs').forEach(function(button){ button.textContent = button.dataset.tmOriginalText || 'Remove from FOBS'; });
      row.querySelectorAll('.provider-delete-cancel').forEach(function(button){ button.textContent = 'Cancel'; });
    }
    tmCertificateDeleteRow = null;
    tmCertificateDeleteIds = [];
  }

  async function refreshTrainerMateLiveStatus(){
    try{
      var r = await fetch('/live-status?_=' + Date.now(), {cache:'no-store'});
      if(!r.ok) throw new Error('HTTP ' + r.status);
      var data = await r.json();

      setText('tmLiveBadge', data.running ? 'syncing' : 'idle');
      setText('tmLiveSyncState', data.sync_state || 'Idle');
      setText('tmLiveProvider', data.current_provider || '-');
      setText('tmLiveCourse', data.current_course || '-');
      setText('tmLiveZoom', data.zoom_result || 'Waiting');

      var list = document.getElementById('tmLiveList');
      if(list){
        list.innerHTML = '';
        (data.rows || []).forEach(function(item){
          list.appendChild(makeRow(item.left, item.right));
        });
      }
      var bubble = document.getElementById('tmProgressBubble');
      var title = document.getElementById('tmProgressTitle');
      var subtitle = document.getElementById('tmProgressSubtitle');
      var state = document.getElementById('tmProgressState');
      var icon = document.getElementById('tmProgressIcon');
      var body = document.getElementById('tmProgressBody');
      var certProgress = document.getElementById('tmCertificateProgress');
      var certTitle = document.getElementById('tmCertificateProgressTitle');
      var certDetail = document.getElementById('tmCertificateProgressDetail');
      var certState = document.getElementById('tmCertificateProgressState');
      var certWorkspace = document.getElementById('tmCertificateWorkspace');
      var certBusyNote = document.getElementById('tmCertificateBusyNote');
      var certBusyTitle = document.getElementById('tmCertificateBusyTitle');
      var certBusyDetail = document.getElementById('tmCertificateBusyDetail');
      if(certWorkspace) certWorkspace.classList.toggle('certificate-has-attention', !!document.querySelector('.certificate-attention'));
      if(certProgress){
        var cert = data.certificate_scan || {};
        var latest = cert.latest || {};
        var certRunning = !!data.certificate_running;
        certProgress.classList.remove('idle','running','error');
        certProgress.classList.add(certRunning ? 'running' : ((latest.status || '').toLowerCase() === 'error' ? 'error' : 'idle'));
        if(certTitle) certTitle.textContent = latest.message || (certRunning ? 'Refreshing FOBS certificates' : 'Certificate refresh idle');
        if(certDetail) certDetail.textContent = latest.detail || (certRunning ? 'Checking provider certificate lists.' : 'Refresh FOBS certificates to check provider files.');
        if(certState) {
          certState.textContent = certRunning ? 'working' : (latest.status || 'idle');
          certState.className = 'status-tag ' + (certRunning ? 'due' : ((latest.status || '').toLowerCase() === 'error' ? 'bad' : 'neutral'));
        }
        var detailText = (latest.detail || '').toLowerCase();
        var messageText = (latest.message || '').toLowerCase();
        var lightCertificateTask = certRunning && (detailText.indexOf('remove the certificate') >= 0 || detailText.indexOf('gone from fobs') >= 0 || messageText.indexOf('removing from') >= 0);
        if(certWorkspace) certWorkspace.classList.toggle('certificate-busy', certRunning && !lightCertificateTask);
        if(certBusyTitle) certBusyTitle.textContent = latest.message || 'TrainerMate is checking certificates';
        if(certBusyDetail) certBusyDetail.textContent = latest.detail || 'Checking the provider certificate lists.';
        document.querySelectorAll('.certificate-job-control').forEach(function(control){
          if(control.closest('.provider-certificate-delete-form.is-working')) return;
          control.disabled = certRunning && !lightCertificateTask;
        });
        if(certRunning){
          markCertificateRefreshRunning();
        } else if(tmCertificateRefreshPending || tmObservedCertificateRunning || tmCertificateRefreshSawRunning){
          var latestStatusLower = (latest.status || '').toLowerCase();
          if(['complete','error','cancelled','idle','skipped'].indexOf(latestStatusLower) >= 0 || latestStatusLower === 'running' || tmCertificateRefreshSawRunning || tmObservedCertificateRunning){
            var finishStatus = latestStatusLower === 'running' ? 'complete' : (latest.status || 'complete');
            if(tmCertificateStayInPlace){
              window.setTimeout(function(){ finishCertificateInPlace(finishStatus); }, 350);
            } else {
              window.setTimeout(reloadCertificatesView, 350);
            }
          }
        }
      }
      document.querySelectorAll('.provider-certificate-delete-form').forEach(function(form){
        if(form.dataset.tmBound === '1') return;
        form.dataset.tmBound = '1';
        form.addEventListener('submit', function(e){
          e.preventDefault();
          if(form.dataset.tmSubmitting === '1') return false;
          form.dataset.tmSubmitting = '1';
          markCertificateInPlaceStarted();
          tmCertificateDeleteRow = form.closest('tr');
          form.classList.add('is-working');
          var btn = form.querySelector('button');
          var cancelBtn = form.querySelector('.provider-delete-cancel');
          if(btn){
            btn.disabled = true;
            if(!btn.dataset.tmOriginalText) btn.dataset.tmOriginalText = btn.textContent || 'Remove from FOBS';
            btn.textContent = 'Removing...';
          }
          if(cancelBtn) cancelBtn.disabled = false;
          if(certProgress){
            certProgress.classList.remove('idle','error');
            certProgress.classList.add('running');
          }
          var providerName = form.dataset.providerName || 'provider';
          if(certTitle) certTitle.textContent = 'Removing from ' + providerName;
          if(certDetail) certDetail.textContent = 'TrainerMate is asking FOBS to remove the certificate.';
          if(certState) {
            certState.textContent = 'working';
            certState.className = 'status-tag due';
          }
          fetch(form.action, {
            method:'POST',
            body:new FormData(form),
            credentials:'same-origin',
            headers:{'Accept':'application/json'}
          })
            .then(function(r){ if(!r.ok) throw new Error('HTTP ' + r.status); return r.json().catch(function(){ return {}; }); })
            .then(function(payload){
              if(payload && payload.confirm_required){
                var lines = [payload.message || 'TrainerMate found matching certificates in other providers. Remove those too?'];
                if(payload.certificate){
                  lines.push('');
                  lines.push('Selected: ' + (payload.certificate.name || 'Certificate') + ' - ' + (payload.certificate.provider || 'provider') + ' - expires ' + (payload.certificate.expiry_date || 'No expiry date shown'));
                }
                if(payload.matching_certificates && payload.matching_certificates.length){
                  lines.push('');
                  lines.push('Other matching copies:');
                  payload.matching_certificates.forEach(function(item){
                    lines.push('- ' + (item.name || 'Certificate') + ' - ' + (item.provider || 'provider') + ' - expires ' + (item.expiry_date || 'No expiry date shown'));
                  });
                }
                lines.push('');
                lines.push('Choose OK to remove all matching provider copies, or Cancel to remove only the selected one.');
                var includeAll = window.confirm(lines.join('\n'));
                var retryData = new FormData(form);
                retryData.set(includeAll ? 'delete_matching' : 'selected_only', '1');
                if(includeAll && payload.matching_certificate_ids){
                  payload.matching_certificate_ids.forEach(function(id){ retryData.append('matching_certificate_ids', id); });
                }
                return fetch(form.action, {
                  method:'POST',
                  body:retryData,
                  credentials:'same-origin',
                  headers:{'Accept':'application/json'}
                }).then(function(r){ if(!r.ok) throw new Error('HTTP ' + r.status); return r.json().catch(function(){ return {}; }); });
              }
              return payload || {};
            })
            .then(function(payload){
              if(payload && payload.started === false) throw new Error(payload.message || 'Not started');
              tmCertificateDeleteIds = (payload && payload.certificate_ids) ? payload.certificate_ids : [];
              refreshTrainerMateLiveStatus();
            })
            .catch(function(){
              form.classList.remove('is-working');
              form.dataset.tmSubmitting = '0';
              if(btn){
                btn.disabled = false;
                btn.textContent = btn.dataset.tmOriginalText || 'Remove from FOBS';
              }
              if(cancelBtn) cancelBtn.disabled = false;
              clearCertificateRefreshMarker();
            });
        }, true);
        var cancel = form.querySelector('.provider-delete-cancel');
        if(cancel && cancel.dataset.tmCancelBound !== '1'){
          cancel.dataset.tmCancelBound = '1';
          cancel.addEventListener('click', function(){
            if(form.dataset.tmSubmitting !== '1') return;
            cancel.disabled = true;
            cancel.textContent = 'Cancelling...';
            if(certDetail) certDetail.textContent = 'Cancelling if FOBS has not removed it yet.';
            fetch(form.dataset.cancelAction || '', {
              method:'POST',
              body:new FormData(form),
              credentials:'same-origin',
              headers:{'Accept':'application/json'}
            }).catch(function(){});
          });
        }
      });
      document.querySelectorAll('.certificate-refresh-form').forEach(function(form){
        if(form.dataset.tmBound === '1') return;
        form.dataset.tmBound = '1';
        form.addEventListener('submit', function(e){
          e.preventDefault();
          markCertificateRefreshStarted();
          var btn = form.querySelector('button');
          if(btn){
            btn.disabled = true;
            btn.textContent = 'Starting...';
          }
          if(certProgress){
            certProgress.classList.remove('idle','error');
            certProgress.classList.add('running');
          }
          if(certTitle) certTitle.textContent = 'Starting certificate refresh';
          if(certDetail) certDetail.textContent = 'TrainerMate will keep checking this for you.';
          if(certState) {
            certState.textContent = 'working';
            certState.className = 'status-tag due';
          }
    fetch(form.action, {method:'POST', body:new FormData(form), credentials:'same-origin', headers:{'X-Requested-With':'fetch','Accept':'application/json'}})
            .then(function(){ refreshTrainerMateLiveStatus(); })
            .catch(function(){ form.submit(); });
        });
      });
      if(bubble && body){
        bubble.classList.remove('idle','running','error');
        bubble.classList.add(data.running ? 'running' : ((data.sync_state || '').toLowerCase().indexOf('error') >= 0 ? 'error' : 'idle'));
        if(title) title.textContent = data.certificate_running ? 'Checking certificates' : (data.running ? 'Syncing TrainerMate' : 'TrainerMate ready');
        if(subtitle) subtitle.textContent = data.running ? (data.progress_summary || data.current_course || data.current_provider || 'Working on this now.') : 'No sync running.';
        if(state) state.textContent = data.running ? 'working' : 'idle';
        if(icon) icon.textContent = data.running ? '...' : 'OK';
        body.innerHTML = '';
        (data.rows || []).slice(0, 8).forEach(function(item){
          var step = document.createElement('div');
          step.className = 'tm-progress-step';
          var dot = document.createElement('i');
          var span = document.createElement('span');
          span.textContent = item.left || '';
          var small = document.createElement('small');
          small.textContent = item.right || '';
          span.appendChild(small);
          step.appendChild(dot);
          step.appendChild(span);
          body.appendChild(step);
        });
      }
    }catch(err){
      // Keep the page calm if the one-second status poll misses while the dashboard is starting/reloading.
      setText('tmLiveBadge', 'idle');
      setText('tmLiveSyncState', 'Waiting for dashboard status');
      var certProgress = document.getElementById('tmCertificateProgress');
      var certTitle = document.getElementById('tmCertificateProgressTitle');
      var certDetail = document.getElementById('tmCertificateProgressDetail');
      var certState = document.getElementById('tmCertificateProgressState');
      if(certProgress){
        certProgress.classList.remove('running','error');
        certProgress.classList.add('idle');
      }
      var certWorkspace = document.getElementById('tmCertificateWorkspace');
      if(certWorkspace) certWorkspace.classList.remove('certificate-busy');
      document.querySelectorAll('.certificate-job-control').forEach(function(control){
        control.disabled = false;
      });
      if(certTitle) certTitle.textContent = 'Dashboard status loading';
      if(certDetail) certDetail.textContent = 'This usually clears after a refresh or restart.';
      if(certState){
        certState.textContent = 'idle';
        certState.className = 'status-tag neutral';
      }
      var list = document.getElementById('tmLiveList');
      if(list){
        list.innerHTML = '';
        list.appendChild(makeRow('Waiting for status', 'Refresh the page if this stays here.'));
      }
    }
  }

  if(document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', refreshTrainerMateLiveStatus);
  } else {
    refreshTrainerMateLiveStatus();
  }
  setInterval(refreshTrainerMateLiveStatus, 1000);
})();
