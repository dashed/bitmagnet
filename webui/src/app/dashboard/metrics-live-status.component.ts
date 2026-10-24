import { Component, EventEmitter, Input, Output } from "@angular/core";
import { AppModule } from "../app.module";
import { TimeAgoPipe } from "../pipes/time-ago.pipe";

export type MetricsAutoRefreshInterval =
  | "off"
  | "seconds_10"
  | "seconds_30"
  | "minutes_1"
  | "minutes_5";

@Component({
  selector: "app-metrics-live-status",
  standalone: true,
  imports: [AppModule, TimeAgoPipe],
  templateUrl: "./metrics-live-status.component.html",
  styleUrl: "./metrics-live-status.component.scss",
})
export class MetricsLiveStatusComponent {
  @Input() autoRefresh: MetricsAutoRefreshInterval = "off";
  @Input() intervals: readonly MetricsAutoRefreshInterval[] = [];
  @Input() loading = false;
  @Input() lastUpdatedAt?: Date;

  @Output() autoRefreshChange = new EventEmitter<MetricsAutoRefreshInterval>();
  @Output() refresh = new EventEmitter<void>();

  get isLive() {
    return this.autoRefresh !== "off";
  }

  get icon() {
    return this.isLive ? "sensors" : "pause_circle";
  }
}
