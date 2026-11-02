import { ButtonItem, PanelSection, PanelSectionRow, ToggleField, staticClasses } from "@decky/ui";
import { callable, definePlugin } from "@decky/api";
import { useEffect, useState } from "react";
import { FaExchangeAlt } from "react-icons/fa";

const command = callable<[any], any>("command");

const comma = (x: number, d = 1) => x.toFixed(d).replace(".", ",");
const gb = (b: number) => (b >= 1e9 ? `${comma(b / 1e9)} ГБ` : `${comma(b / 1e6, 0)} МБ`);
const duration = (s: number) => (s >= 60 ? `${Math.floor(s / 60)} мин` : `${Math.floor(s)} с`);
const ACTIVE = ["running", "reconnecting", "connecting", "interrupted"];
const NUM_KEYS = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "×", "0", "←"];

function Panel() {
  const [st, setSt] = useState<any>(null);
  const [offline, setOffline] = useState(false);
  const [code, setCode] = useState("");
  useEffect(() => {
    let alive = true;
    const tick = async () => {
      const r = await command({ cmd: "status" });
      if (!alive) return;
      setOffline(!r.ok);
      if (r.ok) setSt(r.result);
    };
    tick();
    const t = setInterval(tick, 1000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  if (offline || !st) {
    return (
      <PanelSection title="DeckDrop">
        <PanelSectionRow>{offline ? "Служба не запущена" : "Подключаюсь…"}</PanelSectionRow>
      </PanelSection>
    );
  }

  const job = st.jobs.find((j: any) => ACTIVE.includes(j.state)) ?? st.jobs[0];
  const offer = st.offers[0];
  const mac = st.devices.find((d: any) => d.paired);
  const pair = st.pair;
  const pressKey = (k: string) => {
    if (k === "×") return setCode("");
    if (k === "←") return setCode(code.slice(0, -1));
    const next = code + k;
    if (next.length === 6) {
      command({ cmd: "pair_enter", code: next });
      setCode("");
    } else setCode(next);
  };

  return (
    <>
      {pair && pair.status !== "paired" && (
        <PanelSection title={`Сопряжение: ${pair.peer_name}`}>
          {pair.code ? (
            <PanelSectionRow>
              <div style={{ fontSize: 36, fontFamily: "monospace", textAlign: "center" }}>{pair.code}</div>
            </PanelSectionRow>
          ) : (
            <>
              <PanelSectionRow>
                <div style={{ fontSize: 28, fontFamily: "monospace", textAlign: "center" }}>{code.padEnd(6, "_")}</div>
              </PanelSectionRow>
              {pair.status === "bad_code" && <PanelSectionRow>Код не подошёл</PanelSectionRow>}
              {NUM_KEYS.map((k) => (
                <PanelSectionRow key={k}>
                  <ButtonItem layout="below" onClick={() => pressKey(k)}>
                    {k}
                  </ButtonItem>
                </PanelSectionRow>
              ))}
            </>
          )}
        </PanelSection>
      )}

      {offer && (
        <PanelSection title={`${offer.peer_name} отправляет файлы`}>
          <PanelSectionRow>
            {offer.name}, {gb(offer.total)}, в {offer.dest}
            {offer.from_llm ? " (запрос создан LLM)" : ""}
          </PanelSectionRow>
          <PanelSectionRow>
            <ButtonItem layout="below" onClick={() => command({ cmd: "accept", offer: offer.offer, dest: offer.dest })}>
              Принять
            </ButtonItem>
          </PanelSectionRow>
          <PanelSectionRow>
            <ButtonItem layout="below" onClick={() => command({ cmd: "decline", offer: offer.offer })}>
              Отклонить
            </ButtonItem>
          </PanelSectionRow>
        </PanelSection>
      )}

      {job && (
        <PanelSection title={job.name}>
          {ACTIVE.includes(job.state) && (
            <PanelSectionRow>
              <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
                <span style={{ fontSize: 44, fontFamily: "monospace", color: job.paused ? undefined : "#1A9FFF" }}>{comma(job.speed / 1e6)}</span>
                <span>МБ/с</span>
              </div>
            </PanelSectionRow>
          )}
          <PanelSectionRow>
            <div style={{ width: "100%", boxSizing: "border-box" }}>
              <div style={{ height: 8, borderRadius: 4, background: "#2E3338", overflow: "hidden" }}>
                <div style={{ height: 8, width: `${job.total ? Math.min(100, (job.bytes / job.total) * 100) : 0}%`, background: "#1A9FFF" }} />
              </div>
              <div style={{ marginTop: 6, fontSize: 13 }}>
                {gb(job.bytes)} из {gb(job.total)}
                {job.eta_secs ? `, ещё ${duration(job.eta_secs)}` : ""}
              </div>
            </div>
          </PanelSectionRow>
          <PanelSectionRow>
            {job.state === "done" && job.root ? `Хеш совпал, в среднем ${comma(job.avg_speed / 1e6)} МБ/с` : job.message || `В среднем ${comma(job.avg_speed / 1e6)} МБ/с`}
          </PanelSectionRow>
          {job.role === "send" && <PanelSectionRow>Отправлено повторно: {job.retx}</PanelSectionRow>}
          {ACTIVE.includes(job.state) && (
            <>
              <PanelSectionRow>
                <ButtonItem layout="below" onClick={() => command({ cmd: job.paused ? "resume" : "pause", job: job.id })}>
                  {job.paused ? "Продолжить" : "Пауза"}
                </ButtonItem>
              </PanelSectionRow>
              <PanelSectionRow>
                <ButtonItem layout="below" onClick={() => command({ cmd: "cancel", job: job.id })}>
                  Отменить
                </ButtonItem>
              </PanelSectionRow>
            </>
          )}
        </PanelSection>
      )}

      <PanelSection title="Свободно">
        <PanelSectionRow>
          {st.me.name}: {gb(st.me.roots[0]?.free ?? 0)}
        </PanelSectionRow>
        {mac && (
          <PanelSectionRow>
            {mac.name}: {mac.online ? gb(mac.roots[0]?.free ?? 0) : "не в сети"}
          </PanelSectionRow>
        )}
        {mac && (
          <PanelSectionRow>
            <ToggleField
              label={`Принимать от ${mac.name} без вопроса`}
              checked={mac.auto_accept}
              onChange={(on) => command({ cmd: "auto_accept", peer: mac.id, on })}
            />
          </PanelSectionRow>
        )}
      </PanelSection>
    </>
  );
}

export default definePlugin(() => ({
  name: "DeckDrop",
  titleView: <div className={staticClasses.Title}>DeckDrop</div>,
  content: <Panel />,
  icon: <FaExchangeAlt />,
  onDismount() {},
}));
