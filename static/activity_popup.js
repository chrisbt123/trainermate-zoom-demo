(function(){
  var config = window.TRAINERMATE_ACTIVITY_CONFIG || {};
  var seen = localStorage.getItem('tmActivitySeen') || '';

  function esc(value){
    return String(value || '').replace(/[&<>"']/g, function(c){
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }

  function ensure(){
    var box = document.getElementById('tmActivityBubble');
    if(box) return box;
    var style = document.createElement('style');
    style.textContent = '.tm-activity-bubble{position:fixed;right:22px;bottom:22px;z-index:9999;width:min(420px,calc(100vw - 32px));background:#0f172a;color:#fff;border:1px solid rgba(125,211,252,.38);border-radius:18px;box-shadow:0 18px 60px rgba(0,0,0,.42);padding:16px;display:none}.tm-activity-bubble.show{display:block}.tm-activity-bubble h3{margin:0 0 6px;font-size:17px}.tm-activity-bubble p{margin:0 0 12px;color:#dbeafe;line-height:1.4}.tm-activity-bubble .row{display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap}.tm-activity-bubble a,.tm-activity-bubble button{border:0;border-radius:11px;padding:9px 12px;font-weight:900;text-decoration:none;cursor:pointer}.tm-activity-bubble a{background:#2563eb;color:#fff}.tm-activity-bubble button{background:#1e293b;color:#dbeafe}';
    document.head.appendChild(style);
    box = document.createElement('div');
    box.id = 'tmActivityBubble';
    box.className = 'tm-activity-bubble';
    box.innerHTML = '<h3></h3><p></p><div class="row"><button type="button">Close</button><a href="/activity">View details</a></div>';
    box.querySelector('button').onclick = function(){ box.classList.remove('show'); };
    document.body.appendChild(box);
    return box;
  }

  async function poll(){
    try{
      var res = await fetch('/api/activity?_=' + Date.now(), {cache:'no-store'});
      if(!res.ok) return;
      var data = await res.json();
      document.querySelectorAll('.message-count,.activity-count').forEach(function(b){
        if(data.counts && data.counts.unread){ b.textContent = data.counts.unread; }
      });
      var item = data.popup;
      if(!item || !item.id || item.id === seen) return;
      seen = item.id;
      localStorage.setItem('tmActivitySeen', seen);
      var box = ensure();
      box.querySelector('h3').innerHTML = esc(item.title || 'TrainerMate update');
      box.querySelector('p').innerHTML = esc(item.summary || item.message || '');
      box.classList.add('show');
      fetch('/activity/' + encodeURIComponent(item.id) + '/read', {
        method:'POST',
        headers:{'X-CSRF-Token': config.csrfToken || ''}
      }).catch(function(){});
    }catch(e){}
  }

  window.addEventListener('load', function(){
    poll();
    setInterval(poll, 7000);
  });
})();
