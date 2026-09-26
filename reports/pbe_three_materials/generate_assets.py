"""Generate source-backed tables and thesis-style scientific figures."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import TwoSlopeNorm


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
ROOT = REPO / "docs/reference_data/pbe_20260925"
sys.path.insert(0, str(REPO))
STABLE = REPO / "docs/acceptance_data"
MATS = ("ws2", "mos2", "wse2")
MODELS = ("tece", "prophet", "equiformer-v3")
LABEL = {"ws2": "WS$_2$", "mos2": "MoS$_2$", "wse2": "WSe$_2$"}
PLOT_LABEL = {"ws2": "WS2", "mos2": "MoS2", "wse2": "WSe2"}
MODEL_LABEL = {"tece": "TECE+MS", "prophet": "Prophet+MS", "equiformer-v3": "EquiformerV3+MS"}
COLORS = {"lda": "#6a7078", "pbe": "#1e618a", "tece": "#dc8b31",
          "prophet": "#9b6aab", "equiformer-v3": "#3f977a"}
ROUTES = ("lda", "pbe", *MODELS)
THIRD = "phi_122_mev_per_A3amu32"
FOURTH = "phi_1122_mev_per_A4amu2"


def read(path: Path) -> dict:
    return json.loads(path.read_text())


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fnum(value: float | None, digits: int = 3) -> str:
    return "--" if value is None else f"{value:.{digits}f}"


def save_table(name: str, header: list[str], rows: list[list[str]]) -> None:
    # The surrounding LaTeX figure/table provides the caption and label.
    widths = "l" + "r" * (len(header) - 1)
    lines = [rf"\begin{{tabular}}{{{widths}}}", r"\toprule",
             " & ".join(header) + r" \\", r"\midrule"]
    lines += [" & ".join(map(str, row)) + r" \\" for row in rows]
    lines += [r"\bottomrule", r"\end{tabular}"]
    (HERE / "tables" / f"{name}.tex").write_text("\n".join(lines) + "\n")


def save_fig(fig: plt.Figure, name: str) -> None:
    fig.savefig(HERE / "figures" / f"{name}.pdf", bbox_inches="tight", dpi=240)
    fig.savefig(HERE / "figures" / f"{name}.png", bbox_inches="tight", dpi=180)
    plt.close(fig)


def model_fit(material: str, code: str, model: str, ws: dict, other: dict) -> dict:
    if material == "ws2":
        item = next(x for x in ws["pairs"] if x["pair"]["pair_code"] == code)
        return item["models"][model]["central"]
    grid = next(x for x in other["new_mlff_grids"]
                if x["material"] == material and x["qe_pair"] == code and x["model"] == model)
    return grid["central"]


def generate_tables(data: dict, results: dict, phonons: dict, audit: dict, geometry: dict, density: dict,
                    ws: dict, other: dict, same: dict) -> None:
    rows = []
    for m in MATS:
        g = geometry[m]
        rows.append([LABEL[m], fnum(g["lda"]["a_A"], 4), fnum(g["pbe"]["a_A"], 4),
                     *[fnum(g["mlff"][model]["a_A"], 4) for model in MODELS],
                     fnum(g["pbe"]["relative_to_lda_percent"], 2) + r"\%"])
    save_table("geometry", ["材料", "旧 LDA", "新 PBE", "TECE", "Prophet", "E3", "PBE/LDA"], rows)

    rows = []
    for m in MATS:
        for route, x in (("lda", phonons[m]["lda_vs_pbe"]),
                         *((model, phonons[m]["mlff_vs_pbe"][model]) for model in MODELS)):
            rows.append([LABEL[m], "QE LDA" if route == "lda" else MODEL_LABEL[route],
                         fnum(x["frequency_mae_thz"], 4), fnum(x["frequency_rmse_thz"], 4),
                         fnum(x["frequency_bias_thz"], 4),
                         fnum(x["isolated_mode_median_overlap_squared"], 4),
                         fnum(x["worst_degenerate_subspace_overlap_squared"], 4)])
    save_table("phonons", ["材料", "路线", "MAE", "RMSE", "偏差", "孤立模中位重叠$^2$", "最差简并子空间$^2$"], rows)

    rows = []
    for m in MATS:
        for item in phonons[m]["key_channels"]:
            rows.append([LABEL[m], item["channel_lda"].replace("Gamma", r"$\Gamma$"),
                         fnum(item["pbe_gamma_frequency_thz"], 3),
                         fnum(item["pbe_q_frequency_thz"], 3),
                         fnum(item["gamma_overlap_squared"], 5),
                         fnum(item["q_overlap_squared"], 5)])
    save_table("mode_mapping", ["材料", "物理通道", r"$\nu_\Gamma$", r"$\nu_q$",
                                r"$\mathcal O_\Gamma^2$", r"$\mathcal O_q^2$"], rows)

    for m in MATS:
        for name, key in (("cubic", "phi122_abs"), ("quartic", "phi1122")):
            rows = []
            for item in data["materials"][m]["rows"]:
                rows.append([item["physical_channel"].replace("Gamma", r"$\Gamma$"),
                             fnum(item["lda"][key]), fnum(item["pbe"][key]),
                             *[fnum(item["mlff"][model][key]) for model in MODELS]])
            save_table(f"{name}_{m}", ["声子对", "旧 LDA", "新 PBE", "TECE+MS",
                                        "Prophet+MS", "EquiformerV3+MS"], rows)

    for target in ("lda", "pbe"):
        rows = []
        for m in MATS:
            for model in MODELS:
                e = data["materials"][m]["metrics"][target][model]
                a, b = e["phi122_abs"], e["phi1122"]
                rows.append([LABEL[m], MODEL_LABEL[model], str(a["count"]),
                             fnum(a["mae"]), fnum(a["rmse"]),
                             str(b["count"]), fnum(b.get("mae")), fnum(b.get("rmse"))])
        save_table(f"metrics_{target}", ["材料", "路线", "$n_3$", "三阶 MAE", "三阶 RMSE",
                                          "$n_4$", "四阶 MAE", "四阶 RMSE"], rows)

    rows = []
    for m in MATS:
        ranking = data["materials"][m]["metrics"]["rank_within_selected_set"]
        rows.append([LABEL[m], *[fnum(ranking[model], 2) for model in MODELS]])
    save_table("rank", ["材料", "TECE+MS", "Prophet+MS", "EquiformerV3+MS"], rows)

    rows = []
    for m in MATS:
        for result in results[m]["rows"]:
            qe_fit = result["center_fit"]
            code = data["materials"][m]["rows"][result["rank"] - 1]["lda_pair_code"]
            ml = [model_fit(m, code, model, ws, other) for model in MODELS]
            rows.append([LABEL[m], result["lda_channel"].replace("Gamma", r"$\Gamma$"),
                         fnum(1000 * qe_fit["center_fit_rmse_ev_supercell"], 4),
                         *[fnum(1000 * x["center_fit_rmse_ev_supercell"], 4) for x in ml],
                         fnum(abs(qe_fit["physics"]["phi_112_mev_per_A3amu32"]), 3)])
    save_table("fit_diagnostics", ["材料", "通道", "PBE RMSE", "TECE RMSE", "Prophet RMSE",
                                   "E3 RMSE", r"$|\Phi_{112}|$ PBE"], rows)

    rows = []
    for m in MATS:
        row = results[m]["rows"][0]
        fits = [row[key] for key in ("center_fit", "wide_fit_1p5", "wide_fit_2")]
        vals = [x["physics"][FOURTH] for x in fits]
        rows.append([LABEL[m], *[fnum(v) for v in vals],
                     f"{100*(vals[2]/vals[0]-1):+.2f}" + r"\%",
                     fnum(1000*fits[2]["center_fit_rmse_ev_supercell"], 4)])
    save_table("window", ["材料", r"$|Q|\leq1$", r"$|Q|\leq1.5$", r"$|Q|\leq2$",
                          "中心到宽窗", "宽窗 RMSE"], rows)

    rows = []
    for item in density["rows"]:
        v = item["values_mev_per_A4amu2"]
        rows.append([LABEL[item["material"]],
                     fnum(v["central_5x5_step_0p5_extent_1"], 4),
                     fnum(v["wide_5x5_step_1_extent_2"], 4),
                     fnum(v["wide_9x9_step_0p5_extent_2"], 4),
                     f"{item['density_change_percent']:+.3f}" + r"\%"])
    save_table("density", ["材料", r"5$\times$5 中心", r"5$\times$5 同窗稀疏",
                           r"9$\times$9 同窗密集", "同窗密度变化"], rows)

    rows = []
    for m in MATS:
        p = results[m]["pilot"]
        rows.append([LABEL[m],
                     f"{100*p['relative_change']['cutoff100']:.4f}" + r"\%",
                     f"{100*p['relative_change']['kmesh24']:.5f}" + r"\%",
                     "通过" if p["passed"] else "失败"])
    save_table("pilots", ["材料", r"100$\to$120 Ry", r"24$\to$30 $k$ 网格", r"2\% 门槛"], rows)

    rows = []
    for m in MATS:
        x = audit["by_material_and_atom_count"][m]
        rows.append([LABEL[m], "193", fnum(x["12"]["median_seconds"], 1),
                     fnum(x["27"]["median_seconds"], 1),
                     fnum(x["all"]["node_hours"], 2)])
    save_table("cost", ["材料", "QE 单点", "12 原子中位秒", "27 原子中位秒", "节点时"], rows)

    rows = []
    for amplitude in ("0.5", "1.0"):
        v = same["even_mixed_mev"][amplitude]
        q = float(amplitude)
        rows.append([amplitude, fnum(v["qe"], 5), fnum(v["mattersim"], 5),
                     fnum(4*v["qe"]/q**4, 3), fnum(4*v["mattersim"]/q**4, 3)])
    save_table("m6_contrast", [r"$Q$", r"PBE $\Delta E$", r"MS $\Delta E$",
                               r"PBE $4\Delta E/Q^4$", r"MS $4\Delta E/Q^4$"], rows)


def plot_phonons(phonons: dict) -> None:
    fig, ax = plt.subplots(1, 2, figsize=(11.8, 3.8), layout="constrained")
    xs = np.arange(3)
    for i, route in enumerate(("lda", *MODELS)):
        values = [phonons[m]["lda_vs_pbe"]["frequency_mae_thz"] if route == "lda"
                  else phonons[m]["mlff_vs_pbe"][route]["frequency_mae_thz"] for m in MATS]
        ax[0].bar(xs + (i-1.5)*0.18, values, width=0.17, color=COLORS[route],
                  label="Old QE LDA" if route == "lda" else MODEL_LABEL[route])
    ax[0].set_xticks(xs, [PLOT_LABEL[m] for m in MATS]);ax[0].set_ylabel("Frequency MAE vs QE PBE (THz)")
    ax[0].legend(ncol=2, fontsize=7, frameon=False);ax[0].grid(axis="y",alpha=.2)
    for i, route in enumerate(("lda", *MODELS)):
        values = [phonons[m]["lda_vs_pbe"]["worst_degenerate_subspace_overlap_squared"] if route == "lda"
                  else phonons[m]["mlff_vs_pbe"][route]["worst_degenerate_subspace_overlap_squared"] for m in MATS]
        ax[1].plot(xs, values, "o-", color=COLORS[route], label="Old QE LDA" if route=="lda" else MODEL_LABEL[route])
    ax[1].set_xticks(xs, [PLOT_LABEL[m] for m in MATS]);ax[1].set_ylabel("Worst near-degenerate subspace overlap²")
    ax[1].set_ylim(.94,1.005);ax[1].grid(alpha=.2)
    save_fig(fig,"phonon_summary")


def plot_qgrid(phonons: dict) -> None:
    fig, axes = plt.subplots(3, 3, figsize=(9.9, 9.0), layout="constrained")
    max_err = 0.0; maps = []
    for im,m in enumerate(MATS):
        fp = np.array([x["freqs_thz"] for x in read(ROOT/f"qe_phonon_dataset_{m}.json")["q_points"]])
        for ir,model in enumerate(MODELS):
            fm=np.array([x["freqs_thz"] for x in read(ROOT/f"mlff_phonon_dataset_{m}_{model}.json")["q_points"]])
            mapping=np.asarray(phonons[m]["mlff_vs_pbe"][model]["mapping_pbe_to_candidate_one_based"])-1
            matched=np.take_along_axis(fm,mapping,axis=1)
            err=np.mean(abs(matched-fp),axis=1)
            err[0]=np.mean(abs(matched[0,3:]-fp[0,3:]))
            image=err.reshape(6,6);maps.append((im,ir,image));max_err=max(max_err,float(image.max()))
    for im,ir,image in maps:
        ax=axes[im,ir]
        mesh=ax.imshow(image.T,origin="lower",vmin=0,vmax=max_err,cmap="YlOrRd")
        ax.set_title(f"{PLOT_LABEL[MATS[im]]} / {MODEL_LABEL[MODELS[ir]]}",fontsize=9)
        ax.set_xticks(range(6),[f"{i}/6" for i in range(6)],rotation=45,fontsize=7)
        ax.set_yticks(range(6),[f"{i}/6" for i in range(6)],fontsize=7)
        ax.set_xlabel("q1");ax.set_ylabel("q2")
    fig.colorbar(mesh,ax=axes.ravel().tolist(),shrink=.65,label="Mean |frequency error| at q (THz)")
    save_fig(fig,"qgrid_frequency_error")


def plot_channels(data: dict, key: str, name: str) -> None:
    fig,axes=plt.subplots(3,1,figsize=(10,9.0),layout="constrained")
    for im,m in enumerate(MATS):
        rows=data["materials"][m]["rows"];xs=np.arange(5);ax=axes[im]
        for ir,route in enumerate(ROUTES):
            vals=[(r[route] if route in ("lda","pbe") else r["mlff"][route]).get(key) for r in rows]
            xx=xs+(ir-2)*.15
            for x,v in zip(xx,vals):
                if v is not None:ax.bar(x,v,width=.145,color=COLORS[route],label=("Old QE LDA" if route=="lda" else "New QE PBE" if route=="pbe" else MODEL_LABEL[route]) if x==xx[0] else None)
        ax.axhline(0,color="black",linewidth=.65)
        ax.set_xticks(xs,[r["physical_channel"].split("-")[1] for r in rows])
        ax.set_title(PLOT_LABEL[m],loc="left",fontsize=10)
        ax.grid(axis="y",alpha=.18)
        if key=="phi1122":ax.set_ylabel("Signed Phi1122")
        else:ax.set_ylabel("|Phi122|")
    handles,labels=axes[0].get_legend_handles_labels()
    fig.legend(handles,labels,ncol=5,loc="upper center",bbox_to_anchor=(.5,1.02),fontsize=8,frameon=False)
    save_fig(fig,name)


def plot_scatter(data: dict) -> None:
    fig,axes=plt.subplots(1,2,figsize=(10.4,4.2),layout="constrained")
    for i,key in enumerate(("phi122_abs","phi1122")):
        ax=axes[i];xall=[];yall=[]
        for m in MATS:
            for model in MODELS:
                rows=data["materials"][m]["rows"]
                x=np.array([r["pbe"][key] for r in rows]);y=np.array([r["mlff"][model][key] for r in rows])
                ax.scatter(x,y,marker={"tece":"o","prophet":"s","equiformer-v3":"^"}[model],
                           s=32,alpha=.65,color={"ws2":"#235f84","mos2":"#c7762d","wse2":"#489572"}[m],
                           label=f"{PLOT_LABEL[m]} / {MODEL_LABEL[model]}")
                xall.extend(x);yall.extend(y)
        lower=min(xall+yall);upper=max(xall+yall);span=upper-lower
        ax.plot([lower-.05*span,upper+.05*span],[lower-.05*span,upper+.05*span],"k--",lw=.8)
        ax.set_xlim(lower-.07*span,upper+.07*span);ax.set_ylim(lower-.07*span,upper+.07*span)
        ax.set_xlabel("QE PBE");ax.set_ylabel("Stage1 + MatterSim")
        ax.set_title("Cubic strength |Phi122|" if i==0 else "Signed quartic Phi1122")
        ax.grid(alpha=.2)
    axes[1].annotate("WS2 M6 sign reversal",xy=(-1.414,1.9),xytext=(1.0,7.0),
                     arrowprops={"arrowstyle":"->","lw":.8},fontsize=8)
    handles,labels=axes[0].get_legend_handles_labels()
    fig.legend(handles,labels,loc="upper center",bbox_to_anchor=(.5,1.11),ncol=3,fontsize=7,frameon=False)
    save_fig(fig,"coupling_scatter")


def plot_m6(same: dict) -> None:
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401
    g=np.linspace(-1,1,5);xx,yy=np.meshgrid(g,g)
    qe=np.asarray(same["qe_grid_ev"]);ms=np.asarray(same["mattersim_grid_ev"])
    qe=(qe-qe[2,2])*1000;ms=(ms-ms[2,2])*1000
    err=ms-qe
    fig=plt.figure(figsize=(10.5,7.5),layout="constrained")
    for i,(z,title) in enumerate(((qe,"QE PBE-USPP"),(ms,"MatterSim on identical structures")),1):
        ax=fig.add_subplot(2,2,i,projection="3d")
        ax.plot_surface(xx,yy,z,cmap="viridis",edgecolor="#334455",linewidth=.3,alpha=.9)
        ax.set_zlim(min(qe.min(),ms.min()),max(qe.max(),ms.max()))
        ax.set_xlabel("Q_Gamma");ax.set_ylabel("Q_M");ax.set_zlabel("Delta E (meV)")
        ax.set_title(title,fontsize=9);ax.view_init(elev=25,azim=-58)
    ax=fig.add_subplot(2,2,3)
    lim=max(abs(err.min()),abs(err.max()))
    mesh=ax.pcolormesh(xx,yy,err,cmap="coolwarm",norm=TwoSlopeNorm(vcenter=0,vmin=-lim,vmax=lim),shading="nearest")
    fig.colorbar(mesh,ax=ax,label="MatterSim - QE (meV)",shrink=.8)
    ax.set_xlabel("Q_Gamma");ax.set_ylabel("Q_M");ax.set_title("Relative-energy difference")
    ax=fig.add_subplot(2,2,4)
    for label,key,color in (("QE PBE","qe",COLORS["pbe"]),("MatterSim","mattersim",COLORS["tece"])):
        a=np.array([.5,1.0]);d=np.array([same["even_mixed_mev"][str(v)][key] for v in a])
        ax.plot(a,4*d/a**4,"o-",color=color,label=label)
    ax.axhline(0,color="black",lw=.7);ax.set_xlabel("Common amplitude Q")
    ax.set_ylabel("Effective Phi1122");ax.set_title("Fit-independent even contrast")
    ax.legend(frameon=False);ax.grid(alpha=.2)
    save_fig(fig,"ws2_m6_same_geometry_pes")


def plot_m6_pointwise(same: dict) -> None:
    qe=np.asarray(same["qe_grid_ev"]);ms=np.asarray(same["mattersim_grid_ev"])
    qrel=(qe-qe[2,2])*1000;mrel=(ms-ms[2,2])*1000
    qforce=np.asarray(same["qe_forces_ev_per_A"]).ravel();mforce=np.asarray(same["mattersim_forces_ev_per_A"]).ravel()
    fig,axes=plt.subplots(1,3,figsize=(11.5,3.5),layout="constrained")
    ax=axes[0];ax.scatter(qrel,mrel,color=COLORS["pbe"],s=30,alpha=.75)
    lo=min(qrel.min(),mrel.min());hi=max(qrel.max(),mrel.max());ax.plot([lo,hi],[lo,hi],"k--",lw=.8)
    ax.set_xlabel("QE relative E (meV)");ax.set_ylabel("MatterSim relative E (meV)")
    ax.set_title("25 identical configurations")
    ax=axes[1];ax.hexbin(qforce,mforce,gridsize=30,cmap="Blues",mincnt=1)
    lo=min(qforce.min(),mforce.min());hi=max(qforce.max(),mforce.max());ax.plot([lo,hi],[lo,hi],"k--",lw=.8)
    ax.set_xlabel("QE force component (eV/A)");ax.set_ylabel("MatterSim force component (eV/A)")
    ax.set_title("900 Cartesian components")
    ax=axes[2];z=mrel-qrel;lim=max(abs(z.min()),abs(z.max()))
    mesh=ax.imshow(z,origin="lower",extent=(-1.25,1.25,-1.25,1.25),cmap="coolwarm",vmin=-lim,vmax=lim)
    fig.colorbar(mesh,ax=ax,label="MS - QE (meV)",shrink=.8)
    ax.set_xticks(np.linspace(-1,1,5));ax.set_yticks(np.linspace(-1,1,5))
    ax.set_xlabel("Q_Gamma");ax.set_ylabel("Q_M");ax.set_title("Pointwise relative-E error")
    save_fig(fig,"ws2_m6_pointwise_errors")


def plot_fits(results: dict, data: dict, ws: dict, other: dict) -> None:
    fig,axes=plt.subplots(1,2,figsize=(11,4),layout="constrained")
    ax=axes[0];labels=[];qev=[];msv=[]
    for m in MATS:
        for r in results[m]["rows"]:
            labels.append(f"{PLOT_LABEL[m]}-{r['rank']}")
            qev.append(1000*r["center_fit"]["center_fit_rmse_ev_supercell"])
            code=data["materials"][m]["rows"][r["rank"]-1]["lda_pair_code"]
            msv.append(1000*model_fit(m,code,"tece",ws,other)["center_fit_rmse_ev_supercell"])
    x=np.arange(len(labels));ax.plot(x,qev,"o-",color=COLORS["pbe"],label="QE PBE")
    ax.plot(x,msv,"s-",color=COLORS["tece"],label="TECE+MS")
    ax.set_yscale("log");ax.set_xticks(x,labels,rotation=60,ha="right",fontsize=7)
    ax.set_ylabel("Center-grid fit RMSE (meV/supercell)");ax.set_title("5x5 polynomial residual")
    ax.legend(frameon=False);ax.grid(alpha=.2)
    ax=axes[1]
    for m in MATS:
        r=results[m]["rows"][0]
        fits=[r[z]["physics"][FOURTH] for z in ("center_fit","wide_fit_1p5","wide_fit_2")]
        ax.plot([1,1.5,2],fits,"o-",label=PLOT_LABEL[m])
    ax.set_xticks([1,1.5,2]);ax.set_xlabel("Fit-window half-width in mass-weighted Q")
    ax.set_ylabel("Phi1122 of strongest channel");ax.set_title("Rank-1 quartic window sensitivity")
    ax.legend(frameon=False);ax.grid(alpha=.2)
    save_fig(fig,"fit_residual_window")


def plot_cost(audit: dict) -> None:
    fig,ax=plt.subplots(1,2,figsize=(9.6,3.5),layout="constrained")
    x=np.arange(3);g=audit["by_material_and_atom_count"]
    ax[0].bar(x-.18,[g[m]["12"]["median_seconds"]/60 for m in MATS],.35,label="12 atoms (M/K)",color=COLORS["pbe"])
    ax[0].bar(x+.18,[g[m]["27"]["median_seconds"]/60 for m in MATS],.35,label="27 atoms (L)",color=COLORS["tece"])
    ax[0].set_xticks(x,[PLOT_LABEL[m] for m in MATS]);ax[0].set_ylabel("Median QE point wall time (min)")
    ax[0].legend(frameon=False,fontsize=8);ax[0].grid(axis="y",alpha=.2)
    ax[1].bar(x,[g[m]["all"]["node_hours"] for m in MATS],color=[COLORS["pbe"],COLORS["tece"],COLORS["equiformer-v3"]])
    ax[1].set_xticks(x,[PLOT_LABEL[m] for m in MATS]);ax[1].set_ylabel("QE single-point node-hours")
    ax[1].grid(axis="y",alpha=.2)
    save_fig(fig,"compute_cost")


def plot_density(density: dict) -> None:
    fig, ax = plt.subplots(figsize=(8.0, 3.6), layout="constrained")
    x = np.arange(3)
    window = [item["window_change_percent"] for item in density["rows"]]
    spacing = [item["density_change_percent"] for item in density["rows"]]
    ax.bar(x - .18, window, .35, label="Window: 5x5 |Q|<=1 to 9x9 |Q|<=2, same step 0.5",
           color=COLORS["pbe"])
    ax.bar(x + .18, spacing, .35, label="Density: 5x5 to 9x9, same |Q|<=2",
           color=COLORS["tece"])
    ax.axhline(0, color="black", linewidth=.65)
    ax.set_xticks(x, [PLOT_LABEL[item["material"]] for item in density["rows"]])
    ax.set_ylabel("Change in signed Phi1122 (%)")
    ax.legend(frameon=False, fontsize=8)
    ax.grid(axis="y", alpha=.2)
    save_fig(fig, "displacement_density")


def main() -> None:
    (HERE/"figures").mkdir(exist_ok=True);(HERE/"tables").mkdir(exist_ok=True)
    data=read(ROOT/"coupling_comparison.json")
    results={m:read(ROOT/f"results_{m}.json") for m in MATS}
    phonons={m:read(ROOT/f"{m}_phonon_comparison.json") for m in MATS}
    audit=read(ROOT/"campaign_audit.json")
    geometry=read(ROOT/"structure_comparison.json")
    density=read(HERE/"density_audit.json")
    for row in density["rows"]:
        if any(sha(REPO/path) != digest for path, digest in row["source_sha256"].items()):
            raise ValueError("Stale displacement-density audit")
    same=read(ROOT/"ws2_pbe_geometry_mattersim_m6.json")
    ws=read(STABLE/"ws2_dft_top5.json")
    other=read(STABLE/"model_dft_top5.json")
    if audit["total"]["n"]!=579 or any(len(data["materials"][m]["rows"])!=5 for m in MATS):
        raise ValueError("Final campaign not complete")
    generate_tables(data,results,phonons,audit,geometry,density,ws,other,same)
    plot_phonons(phonons)
    plot_qgrid(phonons)
    plot_channels(data,"phi122_abs","cubic_channels")
    plot_channels(data,"phi1122","quartic_channels")
    plot_scatter(data)
    plot_m6(same);plot_m6_pointwise(same)
    plot_fits(results,data,ws,other)
    plot_cost(audit)
    plot_density(density)
    inputs=[ROOT/"coupling_comparison.json",ROOT/"campaign_audit.json",ROOT/"structure_comparison.json",ROOT/"ws2_pbe_geometry_mattersim_m6.json",
            HERE/"density_audit.json", REPO/"mlff_modepair_workflow/core.py",
            STABLE/"ws2_dft_top5.json",STABLE/"model_dft_top5.json",
            *[ROOT/f"results_{m}.json" for m in MATS],
            *[ROOT/f"selection_{m}.json" for m in MATS],
            *[ROOT/f"{m}_phonon_comparison.json" for m in MATS],
            *[ROOT/f"qe_phonon_dataset_{m}.json" for m in MATS]]
    inputs.extend(ROOT/f"mlff_phonon_dataset_{m}_{model}.json" for m in MATS for model in MODELS)
    (HERE/"sources.json").write_text(json.dumps({str(p.relative_to(REPO)):sha(p) for p in inputs},indent=2)+"\n")
    print(f"Generated {len(list((HERE/'figures').glob('*.pdf')))} figures and {len(list((HERE/'tables').glob('*.tex')))} tables")


if __name__=="__main__":
    main()
