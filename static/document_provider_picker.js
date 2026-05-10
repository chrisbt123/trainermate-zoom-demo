(function(){
  function syncDocumentProviderPicker(){
    var master = document.getElementById('selectAllDocumentProviders');
    var boxes = Array.prototype.slice.call(document.querySelectorAll('.document-provider-checkbox'));
    var helper = document.getElementById('documentProviderHelper');
    if(!master || !boxes.length) return;

    function setHelper(){
      var checked = boxes.filter(function(box){ return box.checked; }).length;
      if(!helper) return;
      if(master.checked) helper.textContent = 'This certificate will be used for every provider.';
      else if(checked === 0) helper.textContent = 'No providers selected yet. Tick the providers that should use this certificate.';
      else helper.textContent = checked + ' of ' + boxes.length + ' provider' + (boxes.length === 1 ? '' : 's') + ' selected.';
    }

    function setLocked(locked){
      boxes.forEach(function(box){
        box.checked = locked ? true : false;
        box.disabled = locked;
        box.setAttribute('aria-disabled', locked ? 'true' : 'false');
        var label = box.closest('.checkbox');
        if(label) label.classList.toggle('is-disabled', locked);
      });
      setHelper();
    }

    function syncMaster(){
      var checked = boxes.filter(function(box){ return box.checked; }).length;
      master.checked = checked === boxes.length;
      master.indeterminate = checked > 0 && checked < boxes.length;
      if(!master.checked){
        boxes.forEach(function(box){
          box.disabled = false;
          box.setAttribute('aria-disabled', 'false');
          var label = box.closest('.checkbox');
          if(label) label.classList.remove('is-disabled');
        });
      }
      setHelper();
    }

    if(master.dataset.tmProviderPickerBound !== '1'){
      master.dataset.tmProviderPickerBound = '1';
      master.addEventListener('change', function(){
        master.indeterminate = false;
        setLocked(master.checked);
      });
    }

    boxes.forEach(function(box){
      if(box.dataset.tmProviderPickerBound === '1') return;
      box.dataset.tmProviderPickerBound = '1';
      box.addEventListener('change', syncMaster);
    });

    if(master.checked) setLocked(true);
    else syncMaster();
  }

  syncDocumentProviderPicker();
  if(document.readyState === 'loading') document.addEventListener('DOMContentLoaded', syncDocumentProviderPicker);
})();
