// Collapse long column lists to the first LIMIT rows. The preference is shared
// across objects and persisted in localStorage. LIMIT must match
// columnCollapseLimit in web.go.
(function () {
  "use strict";
  var KEY = "hclexp:collapseColumns";
  var LIMIT = 10;

  function isCollapsed() {
    var v = localStorage.getItem(KEY);
    return v === null ? true : v === "1"; // default: collapsed
  }

  function setCollapsed(on) {
    localStorage.setItem(KEY, on ? "1" : "0");
  }

  function apply() {
    var on = isCollapsed();

    var toggles = document.querySelectorAll("input.collapse-toggle");
    toggles.forEach(function (t) { t.checked = on; });

    document.querySelectorAll("table.grid.collapsible").forEach(function (tbl) {
      var body = tbl.tBodies[0];
      if (!body) return;
      var rows = Array.prototype.slice.call(body.rows).filter(function (r) {
        return !r.classList.contains("more-row");
      });
      if (rows.length <= LIMIT) return;

      rows.forEach(function (r, i) {
        r.style.display = on && i >= LIMIT ? "none" : "";
      });

      var moreRow = body.querySelector("tr.more-row");
      if (on) {
        if (!moreRow) {
          moreRow = document.createElement("tr");
          moreRow.className = "more-row";
          var td = document.createElement("td");
          td.colSpan = rows[0].cells.length;
          moreRow.appendChild(td);
          body.appendChild(moreRow);
        }
        moreRow.style.display = "";
        moreRow.cells[0].textContent =
          "… " + (rows.length - LIMIT) + " more hidden — toggle to show";
      } else if (moreRow) {
        moreRow.style.display = "none";
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll("input.collapse-toggle").forEach(function (t) {
      t.addEventListener("change", function () {
        setCollapsed(t.checked);
        apply();
      });
    });
    apply();
  });
})();
