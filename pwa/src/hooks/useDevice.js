/**
 * useDevice - Device detection hook for GRID PWA.
 * Detects iOS/Android, PWA standalone mode, and screen size category.
 */
import { useState, useEffect, useCallback } from 'react';

function detectPlatform() {
    const ua = navigator.userAgent || '';
    const platform = navigator.platform || '';

    const isIOS =
        /iPhone|iPad|iPod/.test(ua) ||
        (platform.includes('Mac') && navigator.maxTouchPoints > 1);

    const isAndroid = /Android/.test(ua);

    const isPWA =
        window.matchMedia('(display-mode: standalone)').matches ||
        navigator.standalone === true;

    let platformName = 'unknown';
    if (isIOS) platformName = 'ios';
    else if (isAndroid) platformName = 'android';
    else platformName = 'desktop';

    return { isIOS, isAndroid, isPWA, platform: platformName };
}

function classifyScreen(width) {
    return {
        isMobile: width <= 480,
        isTablet: width > 480 && width <= 1024,
        isDesktop: width > 1024,
        screenWidth: width,
    };
}

export function useDevice() {
    const [platformInfo] = useState(() => detectPlatform());
    const [screenInfo, setScreenInfo] = useState(() =>
        classifyScreen(window.innerWidth)
    );

    const handleResize = useCallback(() => {
        setScreenInfo(classifyScreen(window.innerWidth));
    }, []);

    useEffect(() => {
        window.addEventListener('resize', handleResize);
        return () => window.removeEventListener('resize', handleResize);
    }, [handleResize]);

    return {
        ...platformInfo,
        ...screenInfo,
    };
}
