(function(){
  var calendarEl = document.getElementById('tmCalendar');
  if(!calendarEl) return;

  var modal = document.getElementById('tmCourseModal');
  var close = document.getElementById('tmModalClose');
  var confirmForm = document.getElementById('tmConfirmRemovedForm');
  var openBothBtn = document.getElementById('tmOpenBothBtn');
  var openZoomBtn = document.getElementById('tmOpenZoomBtn');
  var openFobsBtn = document.getElementById('tmOpenFobsBtn');
  var openJoinBtn = document.getElementById('tmOpenJoinBtn');
  var launchStatus = document.getElementById('tmLaunchStatus');
  var currentToolUrls = {};
  var fobsStatusTimer = null;

  function setText(id, value){ var el=document.getElementById(id); if(el) el.textContent=value || '-'; }
  function setLink(el, url){ if(!el) return; if(url){el.href=url;el.classList.remove('disabled');el.removeAttribute('aria-disabled');}else{el.href='#';el.classList.add('disabled');el.setAttribute('aria-disabled','true');} }
  function setLaunchStatus(message, level){
    if(!launchStatus) return;
    launchStatus.textContent = message || '';
    launchStatus.className = 'launch-status' + (level ? ' ' + level : '');
  }
  function stopFobsStatusPolling(){
    if(fobsStatusTimer){
      clearInterval(fobsStatusTimer);
      fobsStatusTimer = null;
    }
  }
  function pollFobsStatus(launchId){
    stopFobsStatusPolling();
    if(!launchId) return;
    var refresh = function(){
      fetch('/calendar/fobs-launch-status/' + encodeURIComponent(launchId), {cache: 'no-store'})
        .then(function(response){ if(!response.ok){ throw new Error('HTTP ' + response.status); } return response.json(); })
        .then(function(data){
          var status = data.status || '';
          if(data.message){ setLaunchStatus(data.message, status === 'error' ? 'error' : ''); }
          if(status === 'opened' || status === 'error'){
            stopFobsStatusPolling();
          }
        })
        .catch(function(err){
          setLaunchStatus('Could not read FOBS launch status: ' + err, 'warn');
          stopFobsStatusPolling();
        });
    };
    refresh();
    fobsStatusTimer = setInterval(refresh, 1000);
  }
  function launchFobsCourse(url){
    if(!url){ return false; }
    setLaunchStatus('Starting authenticated FOBS browser. This can take a few seconds...', '');
    fetch(url, {method: 'GET', credentials: 'same-origin'})
      .then(function(response){
        if(!response.ok){ throw new Error('HTTP ' + response.status); }
        return response.json().catch(function(){ return {}; });
      })
      .then(function(data){
        if(data && data.ok === false){ throw new Error(data.error || 'FOBS launch failed'); }
        setLaunchStatus('FOBS is opening in a separate TrainerMate browser window. It will log in and then open the course summary.', '');
        if(data && data.launchId){ pollFobsStatus(data.launchId); }
      })
      .catch(function(err){ setLaunchStatus('TrainerMate could not start FOBS automatically: ' + err, 'error'); });
    return true;
  }
  function openTwoTabs(firstUrl, secondUrl){
    var zoomUrl = firstUrl || '';
    var fobsUrl = secondUrl || '';
    if(zoomUrl){
      var tab = window.open(zoomUrl, '_blank', 'noopener');
      if(!tab){
        setLaunchStatus('Zoom tab was blocked. Use the separate Open Zoom meeting button, or allow popups for TrainerMate.', 'warn');
      } else {
        setLaunchStatus('Zoom meeting opened. Starting FOBS next...', '');
      }
    }
    if(fobsUrl){
      launchFobsCourse(fobsUrl);
    }
    if(!zoomUrl && !fobsUrl){
      setLaunchStatus('No Zoom or FOBS link is available for this course yet. Run sync/import first.', 'warn');
    }
  }
  function setStatusClass(value){
    var el=document.getElementById('tmModalStatus');
    if(!el) return;
    el.className='status-tag neutral';
    var v=(value||'').toLowerCase();
    if(v.includes('sync due')) el.className='status-tag due';
    else if(v.includes('need')) el.className='status-tag bad';
    else if(v.includes('synced')||v.includes('ready')) el.className='status-tag ok';
    else if(v.includes('later')||v.includes('scheduled')) el.className='status-tag later';
  }
  function openModal(props){
    currentToolUrls = props || {};
    stopFobsStatusPolling();
    setLaunchStatus('', '');
    setText('tmModalProvider', props.provider || 'Course');
    setText('tmModalProviderName', props.provider || '-');
    setText('tmModalTitle', props.courseTitle || 'Course');
    setText('tmModalDate', props.date || '');
    setText('tmModalTime', props.time || '');
    setText('tmModalStatus', props.status || 'Status');
    setStatusClass(props.status || '');
    setText('tmModalAdvice', props.advice || props.note || '');
    var zoomLabel = props.providerManagesZoom ? 'Provider managed' : (props.zoomAccountLabel || 'Linked Zoom account not selected');
    setText('tmModalZoomAccount', zoomLabel);
    setText('tmModalMeetingId', props.meetingId || 'No Zoom meeting yet');
    setText('tmModalFobsSpecific', props.fobsUrl ? (props.fobsUrlIsExact ? 'Course summary' : 'Provider course list') : 'Not available yet');
    setLink(openZoomBtn, props.zoomUrl || props.zoomAccountUrl || '');
    if(openZoomBtn){ openZoomBtn.textContent = props.zoomUrl ? 'Open Zoom meeting' : 'Open Zoom account'; }
    setLink(openFobsBtn, props.fobsLaunchUrl || props.fobsUrl || '');
    if(openFobsBtn){ openFobsBtn.textContent = 'Open FOBS course'; }
    setLink(openJoinBtn, props.meetingLink || '');
    if(openBothBtn){
      openBothBtn.disabled = !(props.zoomUrl || props.fobsLaunchUrl || props.fobsUrl);
      openBothBtn.classList.toggle('disabled', openBothBtn.disabled);
      openBothBtn.textContent = 'Open Zoom meeting + FOBS';
    }
    var helper = document.getElementById('tmModalHelper');
    if(helper){
      helper.textContent = props.zoomUrl
        ? ('Opens the Zoom meeting summary. If Zoom asks you to sign in or switch account, use ' + zoomLabel + '. Then FOBS opens separately.')
        : ('No specific Zoom meeting is stored yet. Open ' + zoomLabel + ' and FOBS to check this course.');
    }
    if(confirmForm){
      if(props.canConfirmRemoved && props.courseId){
        confirmForm.style.display = 'block';
        confirmForm.action = '/course/' + encodeURIComponent(props.courseId) + '/confirm-removed';
      } else {
        confirmForm.style.display = 'none';
        confirmForm.action = '#';
      }
    }
    if(modal) modal.classList.add('open');
  }
  if(openBothBtn) openBothBtn.onclick = function(){
    openTwoTabs(currentToolUrls.zoomUrl || '', currentToolUrls.fobsLaunchUrl || currentToolUrls.fobsUrl || '');
  };
  if(openFobsBtn) openFobsBtn.onclick = function(e){
    var fobsUrl = currentToolUrls.fobsLaunchUrl || currentToolUrls.fobsUrl || '';
    if(fobsUrl && fobsUrl.indexOf('/calendar/open-fobs-course/') === 0){
      e.preventDefault();
      launchFobsCourse(fobsUrl);
    }
  };
  function closeCourseModal(e){
    if(e) e.preventDefault();
    stopFobsStatusPolling();
    if(modal) modal.classList.remove('open');
  }
  if(close) close.addEventListener('click', closeCourseModal);
  if(modal) modal.addEventListener('click', function(e){
    var closeTrigger = e.target && e.target.closest && e.target.closest('[data-calendar-modal-close],#tmModalClose,.modal-close');
    if(e.target === modal || closeTrigger){
      closeCourseModal(e);
    }
  });

  var params = new URLSearchParams(window.location.search);
  var provider = params.get('provider') || 'all';
  var eventsUrl = calendarEl.dataset.eventsUrl || '/calendar-events';

  var calendar = new FullCalendar.Calendar(calendarEl, {
    initialView: 'dayGridMonth',
    height: 'auto',
    firstDay: 1,
    nowIndicator: true,
    headerToolbar: {
      left: 'prev,next today',
      center: 'title',
      right: 'dayGridMonth,timeGridWeek,listMonth'
    },
    eventTimeFormat: {hour: '2-digit', minute: '2-digit', hour12: false},
    slotLabelFormat: {hour: '2-digit', minute: '2-digit', hour12: false},
    dayHeaderFormat: {weekday: 'short'},
    dayMaxEventRows: 4,
    views: { dayGridMonth: { dayMaxEventRows: 4 } },
    events: eventsUrl + '?provider=' + encodeURIComponent(provider),
    eventContent: function(arg) {
      var esc = function(value){return String(value || '').replace(/[&<>]/g, function(c){return ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]);});};
      var p = arg.event.extendedProps || {};
      return {html: '<div class="tm-cal-event"><div class="tm-cal-time">' + esc(arg.timeText) + '</div><div class="tm-cal-title">' + esc(p.courseTitle || arg.event.title || 'Course') + '</div><div class="tm-cal-provider">' + esc(p.provider || '') + (p.status ? ' - ' + esc(p.status) : '') + '</div></div>'};
    },
    eventClick: function(info) {
      info.jsEvent.preventDefault();
      openModal(info.event.extendedProps || {});
    },
    // TRAINERMATE_PROTECTED: calendar-provider-colours
    // Do not remove this block. Month view must force provider colours onto the rendered event element.
    eventDidMount: function(info) {
      var p = info.event.extendedProps || {};
      var color = p.providerColor || info.event.backgroundColor || '#2563eb';
      var textColor = info.event.textColor || '#ffffff';
      info.el.style.backgroundColor = color;
      info.el.style.borderColor = color;
      info.el.style.color = textColor;
      info.el.title = (p.status || '') + (p.note ? ' - ' + p.note : '');
    }
  });
  calendar.render();
})();
