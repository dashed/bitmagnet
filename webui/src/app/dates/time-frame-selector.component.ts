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
import { MatDatepickerModule } from "@angular/material/datepicker";
import { MatNativeDateModule } from "@angular/material/core";
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
    MatDatepickerModule,
    MatNativeDateModule,
    TranslocoModule,
  ],
  template: `
    <ng-container *transloco="let t">
      <div class="time-frame-container">
        <div class="time-frame-inputs">
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

          <button
            mat-stroked-button
            color="primary"
            class="calendar-button"
            (click)="toggleDatePicker()"
            [matTooltip]="t('dates.date_picker')"
          >
            <mat-icon>calendar_today</mat-icon>
            {{ t("dates.calendar") }}
          </button>
        </div>

        <!-- Date Range Picker -->
        <div class="date-picker-container" *ngIf="showDatePicker">
          <div class="date-range-picker">
            <div class="date-picker-field">
              <mat-form-field subscriptSizing="dynamic">
                <mat-label>{{ t("dates.start_date") }}</mat-label>
                <input
                  matInput
                  [matDatepicker]="startPicker"
                  [formControl]="startDateControl"
                />
                <mat-datepicker-toggle
                  matSuffix
                  [for]="startPicker"
                ></mat-datepicker-toggle>
                <mat-datepicker #startPicker></mat-datepicker>
              </mat-form-field>
            </div>

            <div class="date-picker-field">
              <mat-form-field subscriptSizing="dynamic">
                <mat-label>{{ t("dates.end_date") }}</mat-label>
                <input
                  matInput
                  [matDatepicker]="endPicker"
                  [formControl]="endDateControl"
                  [min]="startDateControl.value"
                />
                <mat-datepicker-toggle
                  matSuffix
                  [for]="endPicker"
                ></mat-datepicker-toggle>
                <mat-datepicker #endPicker></mat-datepicker>
              </mat-form-field>
            </div>

            <div class="date-picker-actions">
              <button
                mat-raised-button
                color="primary"
                (click)="applyDateRange()"
                [disabled]="!startDateControl.value || !endDateControl.value"
              >
                {{ t("general.apply") }}
              </button>
              <button mat-stroked-button (click)="toggleDatePicker()">
                {{ t("general.cancel") }}
              </button>
            </div>
          </div>
        </div>

        <!-- Active time frame display -->
        <div
          class="selected-time-frame"
          *ngIf="
            currentTimeFrame &&
            currentTimeFrame.isValid &&
            timeFrameControl.value
          "
        >
          <mat-chip highlighted color="primary">
            <span class="time-frame-label"
              >{{ t("dates.selected_range") }}:</span
            >
            <span class="time-frame-value">{{
              formatTimeFrameDescription(currentTimeFrame)
            }}</span>
            <button matChipRemove (click)="clearTimeFrame()">
              <mat-icon>cancel</mat-icon>
            </button>
          </mat-chip>
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
        gap: 12px;
      }

      .time-frame-inputs {
        display: flex;
        flex-wrap: wrap;
        gap: 16px;
        align-items: flex-start;
      }

      .time-frame-input {
        flex-grow: 1;
        min-width: 200px;
      }

      .preset-selector {
        min-width: 150px;
      }

      .calendar-button {
        height: 36px;
        margin-top: 4px;
      }

      .date-picker-container {
        background-color: rgba(0, 0, 0, 0.03);
        border-radius: 8px;
        padding: 16px;
        margin: 8px 0;
        animation: fadeIn 0.2s ease-in-out;
      }

      @keyframes fadeIn {
        from {
          opacity: 0;
          transform: translateY(-10px);
        }
        to {
          opacity: 1;
          transform: translateY(0);
        }
      }

      .date-range-picker {
        display: flex;
        flex-direction: column;
        gap: 16px;
      }

      .date-picker-field {
        width: 100%;
      }

      .date-picker-field mat-form-field {
        width: 100%;
      }

      .date-picker-actions {
        display: flex;
        gap: 12px;
        margin-top: 8px;
      }

      .selected-time-frame {
        margin-top: 4px;
      }

      .selected-time-frame mat-chip {
        height: auto;
        padding: 8px 12px;
      }

      .time-frame-label {
        font-weight: 500;
        margin-right: 8px;
      }

      .time-frame-value {
        font-weight: normal;
      }

      .error-message {
        margin-top: 4px;
      }

      .error-message mat-chip {
        height: auto;
        padding: 8px 12px;
      }

      @media (min-width: 768px) {
        .date-range-picker {
          flex-direction: row;
          align-items: center;
        }

        .date-picker-field {
          width: auto;
          flex: 1;
        }

        .date-picker-actions {
          margin-top: 0;
        }
      }
    `,
  ],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class TimeFrameSelectorComponent implements OnInit, OnDestroy {
  @Input() initialTimeFrame: string = "";
  @Output() timeFrameChanged = new EventEmitter<TimeFrame>();

  timeFrameControl = new FormControl<string>("");
  startDateControl = new FormControl<Date | null>(null);
  endDateControl = new FormControl<Date | null>(null);
  showDatePicker = false;
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

      // If there's an initial time frame, set the date controls accordingly
      if (this.currentTimeFrame?.isValid) {
        this.startDateControl.setValue(this.currentTimeFrame.startDate);
        this.endDateControl.setValue(this.currentTimeFrame.endDate);
      }
    }

    // Handle form control changes
    this._subscriptions.push(
      this.timeFrameControl.valueChanges
        .pipe(debounceTime(300), distinctUntilChanged())
        .subscribe((value) => {
          if (value !== null) {
            this.updateTimeFrame(value);

            // Update date pickers when text input changes
            if (this.currentTimeFrame?.isValid) {
              this.startDateControl.setValue(this.currentTimeFrame.startDate);
              this.endDateControl.setValue(this.currentTimeFrame.endDate);
            }
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

  toggleDatePicker(): void {
    this.showDatePicker = !this.showDatePicker;

    // If opening the date picker and we have a valid time frame,
    // set the date controls to match the current time frame
    if (this.showDatePicker && this.currentTimeFrame?.isValid) {
      this.startDateControl.setValue(this.currentTimeFrame.startDate);
      this.endDateControl.setValue(this.currentTimeFrame.endDate);
    }
  }

  applyDateRange(): void {
    if (!this.startDateControl.value || !this.endDateControl.value) {
      return;
    }

    const startDate = this.startDateControl.value;
    const endDate = this.endDateControl.value;

    // Format dates for the expression
    const formatDate = (date: Date) => {
      return date.toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
      });
    };

    // Create a date range expression like "Jan 1, 2023 to Jan 31, 2023"
    const expression = `${formatDate(startDate)} to ${formatDate(endDate)}`;

    // Update the time frame control
    this.timeFrameControl.setValue(expression);

    // Close the date picker
    this.showDatePicker = false;
  }

  clearTimeFrame(): void {
    // Reset all controls
    this.timeFrameControl.setValue("");
    this.startDateControl.setValue(null);
    this.endDateControl.setValue(null);
    this.currentTimeFrame = null;
    this.showDatePicker = false;

    this.timeFrameChanged.emit({
      startDate: new Date(),
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
