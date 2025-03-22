import {
  ChangeDetectionStrategy,
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
} from "@angular/core";
import { CommonModule } from "@angular/common";
import { FormControl, ReactiveFormsModule } from "@angular/forms";
import { MatButtonModule } from "@angular/material/button";
import { MatFormFieldModule } from "@angular/material/form-field";
import { MatInputModule } from "@angular/material/input";
import { MatSelectModule } from "@angular/material/select";
import { MatChipsModule } from "@angular/material/chips";
import { MatIconModule } from "@angular/material/icon";
import { MatTooltipModule } from "@angular/material/tooltip";
import { TranslocoModule } from "@jsverse/transloco";
import {
  Subject,
  Subscription,
  debounceTime,
  distinctUntilChanged,
} from "rxjs";
import {
  TimeFrame,
  formatTimeFrameDescription,
  parseTimeFrame,
  timeFramePresets,
} from "./parse-timeframe";

@Component({
  selector: "app-time-frame-selector",
  standalone: true,
  imports: [
    CommonModule,
    ReactiveFormsModule,
    MatButtonModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatChipsModule,
    MatIconModule,
    MatTooltipModule,
    TranslocoModule,
  ],
  template: `
    <ng-container *transloco="let t">
      <div class="time-frame-container">
        <mat-form-field subscriptSizing="dynamic" class="time-frame-input">
          <mat-label>{{ t("dates.time_frame") }}</mat-label>
          <input
            matInput
            type="text"
            [formControl]="timeFrameControl"
            [placeholder]="t('dates.time_frame_placeholder')"
            [matTooltip]="t('dates.time_frame_tooltip')"
          />
          <button
            *ngIf="timeFrameControl.value"
            matSuffix
            mat-icon-button
            aria-label="Clear"
            (click)="clearTimeFrame()"
          >
            <mat-icon>close</mat-icon>
          </button>
        </mat-form-field>

        <div class="time-frame-presets">
          <mat-form-field subscriptSizing="dynamic" class="preset-selector">
            <mat-label>{{ t("dates.presets") }}</mat-label>
            <mat-select (selectionChange)="onPresetSelected($event)">
              <mat-option
                *ngFor="let preset of timeFramePresets"
                [value]="preset.value"
              >
                {{ preset.label }}
              </mat-option>
            </mat-select>
          </mat-form-field>
        </div>

        <div
          class="active-time-frame"
          *ngIf="currentTimeFrame && currentTimeFrame.isValid"
        >
          <mat-chip-listbox>
            <mat-chip highlighted color="primary">
              {{ formatTimeFrameDescription(currentTimeFrame) }}
              <button matChipRemove (click)="clearTimeFrame()">
                <mat-icon>cancel</mat-icon>
              </button>
            </mat-chip>
          </mat-chip-listbox>
        </div>

        <div
          class="error-message"
          *ngIf="currentTimeFrame && !currentTimeFrame.isValid"
        >
          <mat-chip highlighted color="warn">
            {{ t("dates.time_frame_error") }}: {{ currentTimeFrame.error }}
          </mat-chip>
        </div>
      </div>
    </ng-container>
  `,
  styles: [
    `
      .time-frame-container {
        display: flex;
        flex-direction: column;
        gap: 8px;
      }

      .time-frame-input {
        flex-grow: 1;
        min-width: 200px;
      }

      .time-frame-presets {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }

      .preset-selector {
        min-width: 150px;
      }

      .error-message {
        margin-top: 8px;
      }

      .active-time-frame {
        margin-top: 8px;
      }
    `,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TimeFrameSelectorComponent implements OnInit, OnDestroy {
  @Input() initialTimeFrame: string = "";
  @Output() timeFrameChanged = new EventEmitter<TimeFrame>();

  timeFrameControl = new FormControl<string>("");
  currentTimeFrame: TimeFrame | null = null;
  timeFramePresets = timeFramePresets;

  private _manualChange = new Subject<string>();
  private _subscriptions: Subscription[] = [];

  ngOnInit(): void {
    if (this.initialTimeFrame) {
      this.timeFrameControl.setValue(this.initialTimeFrame, {
        emitEvent: false,
      });
      this.updateTimeFrame(this.initialTimeFrame);
    }

    // Handle form control changes
    this._subscriptions.push(
      this.timeFrameControl.valueChanges
        .pipe(debounceTime(300), distinctUntilChanged())
        .subscribe((value) => {
          if (value !== null) {
            this.updateTimeFrame(value);
          }
        }),
    );

    // Handle manual changes (for presets)
    this._subscriptions.push(
      this._manualChange.pipe(distinctUntilChanged()).subscribe((value) => {
        this.timeFrameControl.setValue(value);
        this.updateTimeFrame(value);
      }),
    );
  }

  ngOnDestroy(): void {
    this._subscriptions.forEach((sub) => sub.unsubscribe());
    this._subscriptions = [];
  }

  onPresetSelected(event: { value: string }): void {
    this._manualChange.next(event.value);
  }

  clearTimeFrame(): void {
    this.timeFrameControl.setValue("");
    this.currentTimeFrame = null;
    this.timeFrameChanged.emit({
      startDate: new Date(0),
      endDate: new Date(),
      expression: "",
      isValid: true,
    });
  }

  private updateTimeFrame(value: string): void {
    const timeFrame = parseTimeFrame(value);
    this.currentTimeFrame = timeFrame;
    this.timeFrameChanged.emit(timeFrame);
  }

  // Helper to format time frame description
  formatTimeFrameDescription(timeFrame: TimeFrame): string {
    return formatTimeFrameDescription(timeFrame);
  }
}
