(function(){
  function buildSupportMessage(){
    var subject = document.getElementById('tmSupportSubject');
    var body = document.getElementById('tmSupportBody');
    var summary = document.getElementById('tmSupportSummary');
    var lines = [];
    lines.push('Subject: ' + ((subject && subject.value.trim()) || 'TrainerMate support'));
    var message = body && body.value.trim();
    if(message){ lines.push(''); lines.push(message); }
    if(summary){ lines.push(''); lines.push('Support summary:'); lines.push(summary.innerText.trim()); }
    return lines.join('\n');
  }

  function updateSupportLinks(){
    var hidden = document.getElementById('tmSupportSummaryField');
    if(hidden) hidden.value = buildSupportMessage();
    var url = 'https://wa.me/447368271579?text=' + encodeURIComponent(buildSupportMessage());
    ['tmSupportWhatsApp','tmSupportWhatsAppTop'].forEach(function(id){
      var el = document.getElementById(id);
      if(el) el.href = url;
    });
  }

  document.addEventListener('input', function(e){
    if(e.target && (e.target.id === 'tmSupportSubject' || e.target.id === 'tmSupportBody')) updateSupportLinks();
  });

  document.addEventListener('click', function(e){
    if(e.target && (e.target.id === 'tmCopySupportSummary' || e.target.id === 'tmCopySupportSummaryBottom')){
      var text = buildSupportMessage();
      if(navigator.clipboard && navigator.clipboard.writeText){
        navigator.clipboard.writeText(text).then(function(){
          e.target.textContent = 'Copied';
          window.setTimeout(function(){ e.target.textContent = 'Copy support summary'; }, 1400);
        });
      }
    }
  });

  document.addEventListener('submit', function(e){
    if(!e.target || e.target.id !== 'tmSupportForm') return;
    e.preventDefault();
    updateSupportLinks();
    var form = e.target;
    var result = document.getElementById('tmSupportResult');
    var button = document.getElementById('tmSendSupportMessage');
    if(result) result.textContent = 'Sending to support...';
    if(button){ button.disabled = true; button.textContent = 'Sending'; }
    fetch(form.action, {method:'POST', body:new FormData(form), credentials:'same-origin'})
      .then(function(r){
        return r.json().catch(function(){ return {}; }).then(function(data){
          if(!r.ok || !data.ok) throw new Error(data.message || 'Support message could not be sent.');
          return data;
        });
      })
      .then(function(){
        if(result) result.textContent = 'Sent to support. Replies will appear in TrainerMate messages.';
        var body = document.getElementById('tmSupportBody');
        if(body) body.value = '';
        updateSupportLinks();
      })
      .catch(function(err){
        if(result) result.textContent = err.message || 'Support message could not be sent. WhatsApp is still available.';
      })
      .finally(function(){
        if(button){ button.disabled = false; button.textContent = 'Send to support'; }
      });
  });

  updateSupportLinks();
})();
