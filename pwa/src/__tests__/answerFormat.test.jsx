/**
 * Tests for parseAnswer / AnswerView (pwa/src/components/home/answerFormat.jsx),
 * which turns a GRID /chat/ask answer into the VERDICT / WHY / CONFLICTS /
 * ACTION CALLS / BREAKING sections the stepdad.finance home page renders.
 */
import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, it, expect } from 'vitest';
import { parseAnswer, AnswerView } from '../components/home/answerFormat.jsx';

describe('parseAnswer', () => {
    it('parses a fully-labelled answer into all sections', () => {
        const text = [
            'VERDICT: Apple looks solid this week.',
            'WHY:',
            '- Earnings beat expectations',
            '- Strong iPhone demand',
            'CONFLICTS: Some analysts worry about China sales.',
            'ACTION CALLS:',
            '- Hold your position',
            '- Consider adding on dips',
            'BREAKING: Apple announced a new product today.',
        ].join('\n');

        const out = parseAnswer(text);
        expect(out.verdict).toBe('Apple looks solid this week.');
        expect(out.why).toEqual(['Earnings beat expectations', 'Strong iPhone demand']);
        expect(out.conflicts).toBe('Some analysts worry about China sales.');
        expect(out.actions).toEqual(['Hold your position', 'Consider adding on dips']);
        expect(out.breaking).toBe('Apple announced a new product today.');
    });

    it('leaves sections empty when they are missing from the text', () => {
        const out = parseAnswer('VERDICT: Tesla is having a rough week.');
        expect(out.verdict).toBe('Tesla is having a rough week.');
        expect(out.why).toEqual([]);
        expect(out.conflicts).toBe('');
        expect(out.actions).toEqual([]);
        expect(out.breaking).toBe('');
    });

    it('recognizes alternate section labels (Bottom line, Reasons, What to do)', () => {
        const text = [
            'Bottom line: Markets are calm.',
            'Reasons:',
            '- Low volatility',
            'What to do:',
            '- Stay the course',
        ].join('\n');

        const out = parseAnswer(text);
        expect(out.verdict).toBe('Markets are calm.');
        expect(out.why).toEqual(['Low volatility']);
        expect(out.actions).toEqual(['Stay the course']);
    });

    it('drops unlabelled prose into rest without losing it, and handles malformed/empty text', () => {
        const out = parseAnswer('Just a plain sentence with no labels at all.');
        expect(out.verdict).toBe('');
        expect(out.rest).toEqual(['Just a plain sentence with no labels at all.']);

        expect(parseAnswer('')).toEqual({ verdict: '', why: [], conflicts: '', actions: [], breaking: '', rest: [] });
        expect(parseAnswer(null)).toEqual({ verdict: '', why: [], conflicts: '', actions: [], breaking: '', rest: [] });
        expect(parseAnswer(undefined)).toEqual({ verdict: '', why: [], conflicts: '', actions: [], breaking: '', rest: [] });
        expect(parseAnswer(42)).toEqual({ verdict: '', why: [], conflicts: '', actions: [], breaking: '', rest: [] });
    });

    it('treats a bare bullet line before any section label as a "why" point', () => {
        const out = parseAnswer('- A stray bullet with no preceding label');
        expect(out.why).toEqual(['A stray bullet with no preceding label']);
    });
});

describe('AnswerView', () => {
    it('renders the verdict as the prominent headline', () => {
        render(<AnswerView text="VERDICT: Apple looks solid this week." />);
        expect(screen.getByText('Apple looks solid this week.')).toBeInTheDocument();
    });

    it('falls back to the first unlabelled line as the headline when there is no VERDICT', () => {
        render(<AnswerView text="Markets are quiet today." />);
        expect(screen.getByText('Markets are quiet today.')).toBeInTheDocument();
    });

    it('renders why bullets, conflicts, and action calls together', () => {
        const text = [
            'VERDICT: Hold steady.',
            'WHY:',
            '- Volatility is low',
            'CONFLICTS: Some see a pullback coming.',
            'ACTION CALLS:',
            '- Do nothing for now',
        ].join('\n');
        render(<AnswerView text={text} />);

        expect(screen.getByText('Hold steady.')).toBeInTheDocument();
        expect(screen.getByText('Volatility is low')).toBeInTheDocument();
        expect(screen.getByText(/Some see a pullback coming\./)).toBeInTheDocument();
        expect(screen.getByText('Do nothing for now')).toBeInTheDocument();
        expect(screen.getByText('What to do')).toBeInTheDocument();
    });

    it('renders the breaking banner when present', () => {
        render(<AnswerView text={'BREAKING: Fed announces rate cut.\nVERDICT: Markets should rally.'} />);
        expect(screen.getByText(/Fed announces rate cut\./)).toBeInTheDocument();
    });
});
