// Live test 2026-11-06: Briefings put 503,791 characters into the DOM. The
// view shows one briefing, so a long one is paged; nothing is dropped.
import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import Briefings, { BRIEFING_PAGE_CHARS, paginateText } from '../views/Briefings.jsx';

vi.mock('../api.js', () => ({
    api: {
        getOllamaStatus: vi.fn(), getLatestBriefing: vi.fn(), listBriefings: vi.fn(),
        generateBriefing: vi.fn(), readBriefing: vi.fn(), askOllama: vi.fn(),
    },
}));

const LINE = 'Liquidity line with enough words to look like a real briefing paragraph.';
const huge = Array.from({ length: 7000 }, (_, i) => `${i} ${LINE}`).join('\n');

describe('paginateText', () => {
    it('keeps short text whole and splits long text losslessly under the budget', () => {
        expect(paginateText('short')).toEqual(['short']);
        expect(paginateText(null)).toEqual([]);
        const pages = paginateText(huge);
        expect(pages.length).toBeGreaterThan(20);
        expect(pages.every(p => p.length <= BRIEFING_PAGE_CHARS)).toBe(true);
        expect(pages.join('\n')).toBe(huge);
    });

    it('keeps blank lines that fall on a page boundary', () => {
        const text = ['a'.repeat(15), '', '', 'b'.repeat(15), '', 'c'].join('\n');
        const pages = paginateText(text, 16);
        expect(pages.every(p => p.length <= 16)).toBe(true);
        expect(pages.join('\n')).toBe(text);
    });

    it('hard-splits a single over-long line', () => {
        const pages = paginateText('x'.repeat(BRIEFING_PAGE_CHARS * 2 + 5));
        expect(pages.map(p => p.length)).toEqual([BRIEFING_PAGE_CHARS, BRIEFING_PAGE_CHARS, 5]);
    });
});

describe('Briefings view', () => {
    beforeEach(() => {
        vi.resetAllMocks();
        api.getOllamaStatus.mockResolvedValue({ available: true, model: 'qwen' });
        api.listBriefings.mockResolvedValue({ briefings: [] });
    });

    it('pages a ~500k-character briefing instead of rendering it all', async () => {
        api.getLatestBriefing.mockResolvedValue({ content: huge, type: 'hourly', timestamp: '2026-11-06T00:00:00Z' });
        const { container } = render(<Briefings />);
        const pager = await screen.findByTestId('briefing-pager');
        const total = paginateText(huge).length;
        expect(pager.textContent).toContain(`Page 1 of ${total}`);
        expect(pager.textContent).toContain(`${huge.length.toLocaleString('en-US')} characters`);
        expect(container.textContent.length).toBeLessThan(BRIEFING_PAGE_CHARS + 5000);
        expect(container.textContent).toContain(`0 ${LINE}`);
        fireEvent.click(screen.getByRole('button', { name: 'Next' }));
        expect(screen.getByTestId('briefing-pager').textContent).toContain(`Page 2 of ${total}`);
        expect(container.textContent).not.toContain(`0 ${LINE}1 `);
    });

    it('says the briefing failed instead of "No hourly briefing available"', async () => {
        api.getLatestBriefing.mockResolvedValue({ error: true, status: 500, message: 'briefing dir unreadable' });
        api.listBriefings.mockResolvedValue({ error: true, status: 500, message: 'list failed' });
        render(<Briefings />);
        expect(await screen.findByText('Briefing unavailable: briefing dir unreadable')).toBeInTheDocument();
        expect(await screen.findByText('Briefing archive unavailable: list failed')).toBeInTheDocument();
        expect(screen.queryByText(/No hourly briefing available/)).toBeNull();
        expect(screen.queryByText('No saved briefings')).toBeNull();
    });

    it('shows loading, not "No briefing", before the first response', () => {
        api.getLatestBriefing.mockImplementation(() => new Promise(() => {}));
        api.listBriefings.mockImplementation(() => new Promise(() => {}));
        render(<Briefings />);
        expect(screen.getByText('Loading hourly briefing...')).toBeInTheDocument();
        expect(screen.getByText('Loading saved briefings...')).toBeInTheDocument();
    });
});
