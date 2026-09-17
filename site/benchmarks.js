/* Progressive enhancement only: all evidence remains readable without JS. */
document.documentElement.classList.add("js");

const modeButtons = [...document.querySelectorAll("[data-timing-mode]")];
function selectMode(button) {
  for (const candidate of modeButtons) {
    const selected = candidate === button;
    candidate.setAttribute("aria-selected", String(selected));
    candidate.tabIndex = selected ? 0 : -1;
    document.getElementById(candidate.getAttribute("aria-controls")).hidden = !selected;
  }
}
for (const button of modeButtons) {
  button.addEventListener("click", () => selectMode(button));
  button.addEventListener("keydown", (event) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const next = event.key === "Home" ? modeButtons[0]
      : event.key === "End" ? modeButtons.at(-1)
      : modeButtons[(modeButtons.indexOf(button) + 1) % modeButtons.length];
    selectMode(next);
    next.focus();
  });
}
if (modeButtons.length) selectMode(modeButtons[0]);

const search = document.getElementById("scenario-search");
if (search) {
  search.addEventListener("input", () => {
    const query = search.value.trim().toLowerCase();
    const groups = [...document.querySelectorAll("[data-scenario]")];
    for (const group of groups) group.hidden = !group.dataset.scenario.includes(query);
    const count = groups.filter(group => !group.hidden).length;
    document.getElementById("scenario-count").textContent =
      `${count} of ${groups.length} scenario groups shown`;
  });
}
