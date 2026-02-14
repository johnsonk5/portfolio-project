(function () {
  const chartEl = document.getElementById("chart");
  const tooltip = document.getElementById("tooltip");
  let hasPlot = false;

  function postMessageToParent(payload) {
    window.parent.postMessage(
      {
        isStreamlitMessage: true,
        ...payload,
      },
      "*"
    );
  }

  function setFrameHeight(height) {
    postMessageToParent({
      type: "streamlit:setFrameHeight",
      height: height,
    });
  }

  function moveTooltip(evt) {
    if (!evt) return;
    const e = evt.event || evt;
    const offset = 12;
    const width = tooltip.offsetWidth || 180;
    const height = tooltip.offsetHeight || 48;
    const rect = chartEl.getBoundingClientRect();
    const containerLeft = rect.left;
    const containerRight = rect.right;
    const containerTop = rect.top;
    const containerBottom = rect.bottom;

    let left = e.clientX + offset;
    let top = e.clientY + offset;
    if (left + width > containerRight) {
      left = e.clientX - width - offset;
    }
    if (top + height > containerBottom) {
      top = e.clientY - height - offset;
    }
    if (left < containerLeft) {
      left = containerLeft + offset;
    }
    if (top < containerTop) {
      top = containerTop + offset;
    }
    tooltip.style.left = left + "px";
    tooltip.style.top = top + "px";
  }

  function attachHandlers(openInNewTab, chartType) {
    if (chartEl.removeAllListeners) {
      chartEl.removeAllListeners("plotly_hover");
      chartEl.removeAllListeners("plotly_unhover");
      chartEl.removeAllListeners("plotly_click");
    }

    chartEl.on("plotly_hover", function (data) {
      if (!data || !data.points || !data.points.length) return;
      const pt = data.points[0];
      let symbol;
      let value;
      let suffix = "";
      let label = "Return: ";
      if (chartType === "horizontal") {
        symbol = pt.y;
        value = typeof pt.x === "number" ? pt.x : Number(pt.x);
        suffix = "%";
      } else {
        symbol = pt.x;
        value = typeof pt.y === "number" ? pt.y : Number(pt.y);
        suffix =
          pt.data && pt.data.meta && pt.data.meta.suffix ? pt.data.meta.suffix : "";
        label = pt.data && pt.data.name ? pt.data.name + ": " : "Value: ";
      }
      if (!symbol || Number.isNaN(value)) return;
      tooltip.innerHTML =
        "Symbol: " + symbol + "<br>" + label + value.toFixed(2) + suffix;
      tooltip.style.display = "block";
      moveTooltip(data.event);
    });

    chartEl.on("plotly_unhover", function () {
      tooltip.style.display = "none";
    });

    chartEl.addEventListener("mousemove", function (evt) {
      if (tooltip.style.display !== "none") {
        moveTooltip(evt);
      }
    });

    chartEl.on("plotly_click", function (data) {
      if (!data || !data.points || !data.points.length) return;
      const symbol = chartType === "horizontal" ? data.points[0].y : data.points[0].x;
      if (!symbol) return;
      if (openInNewTab) {
        const target = "/Deep_Dive?symbol=" + encodeURIComponent(symbol);
        window.open(target, "_blank");
        return;
      }
      postMessageToParent({
        type: "streamlit:setComponentValue",
        value: { symbol: symbol },
        dataType: "json",
      });
    });
  }

  function render(args) {
    const data = args || {};
    const height = data.height || 320;
    const openInNewTab = !!data.open_in_new_tab;
    const chartType = data.chart_type || "horizontal";

    let traces = [];
    let layout = {
      height: height,
      margin: { l: 52, r: 12, t: 8, b: 32 },
      paper_bgcolor: "#000000",
      plot_bgcolor: "#000000",
      font: { color: "#f4f7ff" },
      hovermode: "closest",
      showlegend: false,
      clickmode: "event+select",
    };

    if (chartType === "grouped") {
      const categories = data.categories || [];
      const series = data.series || [];
      const yRange = data.y_range || null;
      const y2Range = data.y2_range || null;
      const barGap = data.bar_gap ?? 0.4;
      const groupGap = data.group_gap ?? 0.2;
      traces = series.map((item) => ({
        x: categories,
        y: item.values || [],
        type: "bar",
        name: item.name || "",
        marker: { color: item.colors || "#60a5fa" },
        hoverinfo: "none",
        cliponaxis: false,
        offsetgroup: item.name || "",
        yaxis: item.yaxis || "y",
        meta: { suffix: item.suffix || "" },
      }));
      layout = {
        ...layout,
        margin: { l: 46, r: 24, t: 6, b: 34 },
        barmode: "group",
        bargap: barGap,
        bargroupgap: groupGap,
        xaxis: {
          title: "",
          tickfont: { color: "#f4f7ff" },
          tickangle: -30,
          tickmode: "array",
          tickvals: categories,
          ticktext: categories,
          tickpadding: 8,
        },
        yaxis: {
          title: data.yaxis_title || "Sentiment / 5D Return (%)",
          titlefont: { size: 10, color: "#f4f7ff" },
          tickfont: { size: 10, color: "#f4f7ff" },
          gridcolor: "rgba(255,255,255,0.08)",
          zeroline: true,
          zerolinecolor: "rgba(255,255,255,0.35)",
          range: yRange || undefined,
        },
        yaxis2: {
          title: data.yaxis2_title || "5D Return (%)",
          titlefont: { size: 12, color: "#f4f7ff" },
          tickfont: { size: 11, color: "#f4f7ff" },
          overlaying: "y",
          side: "right",
          gridcolor: "rgba(255,255,255,0.05)",
          showgrid: false,
          zeroline: true,
          zerolinecolor: "rgba(255,255,255,0.35)",
          range: y2Range || undefined,
        },
      };
    } else {
      const x = data.x || [];
      const y = data.y || [];
      const colors = data.colors || [];
      const xRange = data.x_range || [-1, 1];
      traces = [
        {
          x: x,
          y: y,
          type: "bar",
          orientation: "h",
          marker: { color: colors },
          hoverinfo: "none",
          cliponaxis: false,
        },
      ];
      layout = {
        ...layout,
        xaxis: {
          title: "Daily return (%)",
          type: "linear",
          gridcolor: "rgba(255,255,255,0.08)",
          zeroline: true,
          zerolinecolor: "rgba(255,255,255,0.35)",
          tickformat: ".2f",
          showticklabels: true,
          automargin: true,
          title_standoff: 22,
          range: xRange,
        },
        yaxis: {
          title: "",
          automargin: true,
          autorange: "reversed",
          categoryorder: "array",
          categoryarray: y,
        },
      };
    }

    const config = { displayModeBar: false, responsive: true };

    if (!window.Plotly) {
      return;
    }

    if (!hasPlot) {
      Plotly.newPlot(chartEl, traces, layout, config).then(function () {
        attachHandlers(openInNewTab, chartType);
        hasPlot = true;
        setFrameHeight(height + 10);
      });
    } else {
      Plotly.react(chartEl, traces, layout, config).then(function () {
        attachHandlers(openInNewTab, chartType);
        setFrameHeight(height + 10);
      });
    }
  }

  window.addEventListener("message", function (event) {
    const msg = event.data || {};
    if (msg.type === "streamlit:render") {
      render(msg.args || {});
    }
  });

  postMessageToParent({ type: "streamlit:componentReady", apiVersion: 1 });
  setFrameHeight(10);
})();
